package vn.huyqt.logbroker.transport.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.RequestContext;
import vn.huyqt.logbroker.broker.RequestDispatcher;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolCodec;
import vn.huyqt.logbroker.protocol.ProtocolException;
import vn.huyqt.logbroker.transport.ServerTransport;

/**
 * Bounded Netty TCP adapter. All storage work is delegated outside event loops.
 *
 * <p>Event loops only frame bytes with {@link BoundedFrameDecoder}. Request decoding and
 * response encoding run on a fixed validation pool with a bounded queue; dispatch goes to the
 * {@link RequestDispatcher}. Limits from {@link BrokerConfig} (see
 * {@code docs/broker-configuration.md}) cover connections, frame and decoded request bytes,
 * validation tasks, live request contexts, and outbound bytes per connection and in total.
 * Exceeding a limit, or any malformed framing, closes the offending connection; requests are
 * never queued without bound.
 *
 * <p>{@link #start}, {@link #stopAccepting} and {@link #closeAsync} are synchronized.
 */
public final class NettyServerTransport implements ServerTransport {
    private final BrokerConfig config;
    private final DeadlineScheduler clock;
    private final ProtocolCodec codec;
    private final ResourceBudget connectionBudget;
    private final ResourceBudget inputBudget;
    private final ResourceBudget outboundBudget;
    private final ResourceBudget contextBudget;
    private final ThreadPoolExecutor validation;
    private final AtomicLong connectionIds = new AtomicLong();
    private final Set<Channel> connections = ConcurrentHashMap.newKeySet();
    private MultiThreadIoEventLoopGroup boss;
    private MultiThreadIoEventLoopGroup loops;
    private Channel listener;
    private RequestDispatcher dispatcher;
    private CompletableFuture<Void> closing;

    public NettyServerTransport(BrokerConfig config, DeadlineScheduler clock) {
        this(config, clock, new ResourceBudget(config.maxQueuedRequestBytes()));
    }

    /**
     * @param inputBudget caller-supplied budget for framed and decoded request bytes, used
     *     instead of one sized from {@link BrokerConfig#maxQueuedRequestBytes()}
     */
    public NettyServerTransport(BrokerConfig config, DeadlineScheduler clock,
            ResourceBudget inputBudget) {
        this.config = config;
        this.clock = clock;
        codec = new ProtocolCodec(config.protocolLimits());
        connectionBudget = new ResourceBudget(config.maxConnections());
        this.inputBudget = inputBudget;
        outboundBudget = new ResourceBudget(config.maxOutboundTotal());
        contextBudget = new ResourceBudget(config.maxRequestContexts());
        validation = new ThreadPoolExecutor(config.validationWorkers(), config.validationWorkers(),
                // Bounded queue with AbortPolicy: a full queue surfaces as
                // RejectedExecutionException, which closes the connection instead of blocking.
                0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(config.maxValidationTasks()),
                task -> {
                    var thread = new Thread(task, "broker-validation");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * {@inheritDoc}
     *
     * <p>Binding blocks the caller. On failure both event loop groups are shut down before the
     * exception is thrown.
     *
     * @throws IllegalStateException if the transport was already started
     */
    @Override
    public synchronized InetSocketAddress start(InetSocketAddress bind,
            RequestDispatcher dispatcher)
            throws IOException {
        if (listener != null)
            throw new IllegalStateException("Transport already started");
        this.dispatcher = dispatcher;
        boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        loops = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        try {
            var bootstrap = new ServerBootstrap().group(boss, loops)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline().addLast(new BoundedFrameDecoder(
                                    config.protocolLimits(), inputBudget, clock));
                            channel.pipeline().addLast(new ConnectionHandler());
                        }
                    });
            ChannelFuture bound = bootstrap.bind(bind).syncUninterruptibly();
            if (!bound.isSuccess())
                throw new IOException("Cannot bind broker", bound.cause());
            listener = bound.channel();
            return (InetSocketAddress) listener.localAddress();
        } catch (RuntimeException | IOException error) {
            boss.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            loops.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            if (error instanceof IOException io)
                throw io;
            throw new IOException("Broker listener failed", error);
        }
    }

    @Override
    public synchronized void stopAccepting() {
        if (listener != null)
            listener.close();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Runs on the common pool: closes the listener and every connection, waits up to
     * {@link BrokerConfig#shutdownTimeout()} for validation tasks before forcing them to stop,
     * then shuts down the event loops. Responses not yet written when a connection closes are
     * dropped. The future completes exceptionally if the closing thread is interrupted.
     */
    @Override
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closing != null)
            return closing;
        closing = CompletableFuture.runAsync(() -> {
            stopAccepting();
            for (Channel channel : Set.copyOf(connections))
                channel.close();
            validation.shutdown();
            try {
                if (!validation.awaitTermination(config.shutdownTimeout().toMillis(),
                        TimeUnit.MILLISECONDS))
                    validation.shutdownNow();
                if (loops != null)
                    loops.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
                if (boss != null)
                    boss.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted closing Netty transport", error);
            }
        });
        return closing;
    }

    /**
     * Per-connection request pipeline. Frames are decoded one at a time on the validation pool,
     * in arrival order, so requests from one connection reach the dispatcher in the order they
     * were received. Responses may still complete and be written out of order. The frame queue
     * and decoding flag are guarded by this handler's monitor because they are touched from
     * both the event loop and validation threads.
     */
    private final class ConnectionHandler extends ChannelInboundHandlerAdapter {
        private final long id = connectionIds.incrementAndGet();
        private final ArrayDeque<BoundedFrameDecoder.OwnedFrame> received = new ArrayDeque<>();
        private final Set<Long> activeIds = ConcurrentHashMap.newKeySet();
        private final ResourceBudget perConnectionOutbound = new ResourceBudget(config.maxOutboundPerConnection());
        private ResourceBudget.Lease connectionLease;
        private boolean decoding;

        // Connections beyond maxConnections are accepted and then closed immediately.
        @Override
        public void channelActive(ChannelHandlerContext context) {
            connectionLease = connectionBudget.reserve(1).orElse(null);
            if (connectionLease == null) {
                context.close();
                return;
            }
            connections.add(context.channel());
            context.fireChannelActive();
        }

        @Override
        public synchronized void channelRead(ChannelHandlerContext context, Object message) {
            if (!(message instanceof BoundedFrameDecoder.OwnedFrame frame)) {
                context.close();
                return;
            }
            // Cap frames waiting behind this connection's decode so a fast sender cannot queue
            // without bound; each queued frame still holds its input reservation.
            if (received.size() >= config.maxValidationTasks()) {
                frame.close();
                context.close();
                return;
            }
            received.addLast(frame);
            scheduleDecode(context);
        }

        private synchronized void scheduleDecode(ChannelHandlerContext context) {
            if (decoding || received.isEmpty())
                return;
            decoding = true;
            var frame = received.removeFirst();
            try {
                // The next frame is scheduled only after this one is dispatched, which keeps
                // per-connection order while decoding runs off the event loop.
                validation.execute(() -> {
                    try {
                        decode(context, frame);
                    } finally {
                        frame.close();
                        synchronized (ConnectionHandler.this) {
                            decoding = false;
                            scheduleDecode(context);
                        }
                    }
                });
            } catch (RejectedExecutionException rejected) {
                decoding = false;
                frame.close();
                context.close();
            }
        }

        private void decode(ChannelHandlerContext context, BoundedFrameDecoder.OwnedFrame owned) {
            byte[] bytes = owned.bytes();
            // Decoded objects are charged to the same input budget before decoding, and the
            // lease lives until the request completes, so in-process requests count as queued.
            ResourceBudget.Lease decoded = inputBudget.reserve(
                    codec.estimatedDecodedBytes(bytes)).orElse(null);
            if (decoded == null) {
                context.close();
                return;
            }
            boolean transferred = false;
            try {
                Protocol.RequestFrame request;
                try {
                    request = codec.decodeRequest(bytes);
                } catch (ProtocolException malformed) {
                    // The decoder's 12-byte minimum guarantees the header is present, so a bad
                    // body still gets an error reply echoing it. Request IDs must be >= 0
                    // (docs/protocol-v1.md); a negative one closes the connection instead.
                    short operation = ByteBuffer.wrap(bytes).getShort(4);
                    short version = ByteBuffer.wrap(bytes).getShort(6);
                    long requestId = ByteBuffer.wrap(bytes).getLong(8);
                    if (requestId < 0) {
                        context.close();
                        return;
                    }
                    respond(context, new Protocol.ResponseFrame(operation, version, requestId,
                            new Protocol.Failure(new Protocol.Error(malformed.code(),
                                    malformed.getMessage()))));
                    return;
                }
                // At most 32 requests in flight per connection. IDs must be unique while in
                // flight (docs/protocol-v1.md); a duplicate closes the connection.
                if (activeIds.size() >= 32 || !activeIds.add(request.requestId())) {
                    context.close();
                    return;
                }
                ResourceBudget.Lease contextLease = contextBudget.reserve(1).orElse(null);
                if (contextLease == null) {
                    activeIds.remove(request.requestId());
                    context.close();
                    return;
                }
                // Broker processing deadline starts after the request is received and admitted.
                long deadline = clock.nanoTime() + Duration.ofSeconds(30).toNanos();
                var requestContext = new RequestContext(id, request.requestId(), deadline);
                dispatcher.handle(requestContext, request.body()).whenComplete((reply, error) -> {
                    activeIds.remove(request.requestId());
                    contextLease.close();
                    decoded.close();
                    // Failures are mapped to a generic STORAGE_ERROR so every admitted request
                    // gets a reply while the connection is open.
                    if (!context.channel().isActive())
                        return;
                    Protocol.Response response = error == null ? reply
                            : new Protocol.Failure(new Protocol.Error(ErrorCode.STORAGE_ERROR,
                                    "Broker request failed"));
                    respond(context, new Protocol.ResponseFrame(request.operation(),
                            request.version(), request.requestId(), response));
                });
                transferred = true;
            } finally {
                if (!transferred)
                    decoded.close();
            }
        }

        private void respond(ChannelHandlerContext context, Protocol.ResponseFrame reply) {
            try {
                validation.execute(() -> {
                    if (!context.channel().isActive())
                        return;
                    // Encoding stays off the event loop. Outbound bytes are held against both
                    // budgets until the write completes, so a slow reader that exhausts either
                    // one is disconnected rather than buffered without bound.
                    try {
                        byte[] bytes = codec.encodeResponse(reply);
                        ResourceBudget.Lease global = outboundBudget.reserve(bytes.length).orElse(null);
                        ResourceBudget.Lease local = perConnectionOutbound.reserve(bytes.length).orElse(null);
                        if (global == null || local == null) {
                            if (global != null)
                                global.close();
                            if (local != null)
                                local.close();
                            context.close();
                            return;
                        }
                        context.writeAndFlush(Unpooled.wrappedBuffer(bytes)).addListener(done -> {
                            local.close();
                            global.close();
                            if (!done.isSuccess())
                                context.close();
                        });
                    } catch (ProtocolException failure) {
                        context.close();
                    }
                });
            } catch (RejectedExecutionException overloaded) {
                context.close();
            }
        }

        // Cancels this connection's request contexts in the dispatcher, then returns the
        // connection slot and the reservations of frames that were never decoded.
        @Override
        public synchronized void channelInactive(ChannelHandlerContext context) {
            dispatcher.disconnect(id);
            connections.remove(context.channel());
            if (connectionLease != null)
                connectionLease.close();
            while (!received.isEmpty())
                received.removeFirst().close();
            context.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            context.close();
        }
    }
}
