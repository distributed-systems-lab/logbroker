package vn.huyqt.logbroker.controller.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Frame;
import vn.huyqt.logbroker.controller.transport.QuorumFrameDecoder;
import vn.huyqt.logbroker.broker.cluster.BrokerClusterConfig;
import vn.huyqt.logbroker.transport.netty.BoundedFrameDecoder;

/**
 * Shared factory budgets and workers; each connection owns one channel.
 *
 * <p>Inbound frames are framed by {@link QuorumFrameDecoder}, then preflighted and decoded with
 * {@link QuorumCodec} on a decode worker, under the limits of {@link ControllerConfig#defaults}
 * for the target identity. Broker discovery uses bounded wire-only framing; its control client
 * verifies and pins membership before accepting metadata. Quorum voter framing remains strict.
 * A decode failure closes the channel and reports through the failure callback.
 */
public final class NettyControllerClientTransport implements ControllerClientTransport {
  /**
   * Owns what all attempts share: two I/O threads, two decode threads with a 1024-task queue, and
   * 64 MiB inbound and outbound byte budgets. Closing it closes every transport it created.
   */
  public static final class Factory implements ControllerClientTransport.Factory {
    private final ControllerConfig config;
    private final QuorumCodec.WireLimits wireLimits;
    private final BrokerClusterConfig broker;
    private final DeadlineScheduler clock;
    private final MultiThreadIoEventLoopGroup loops =
        new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final ThreadPoolExecutor decode =
        new ThreadPoolExecutor(
            2,
            2,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1024),
            r -> new Thread(r, "controller-client-decode"));
    private final ResourceBudget inbound = new ResourceBudget(64L * 1024 * 1024),
        outbound = new ResourceBudget(64L * 1024 * 1024);
    private final Set<NettyControllerClientTransport> transports = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    /**
     * Starts the shared threads immediately; {@link #close()} stops them.
     *
     * @param clock caller-owned; used for the decoder's partial-frame deadline
     */
    public Factory(ClusterIdentity identity, DeadlineScheduler clock) {
      config = ControllerConfig.defaults(identity);
      wireLimits = QuorumCodec.WireLimits.from(config);
      broker = null;
      this.clock = clock;
    }

    /** Broker discovery has no local voter identity; the control client pins reply membership. */
    public Factory(BrokerClusterConfig broker, DeadlineScheduler clock) {
      this.broker = Objects.requireNonNull(broker);
      this.config = null;
      this.wireLimits = QuorumCodec.WireLimits.observer(broker.limits());
      this.clock = Objects.requireNonNull(clock);
    }

    public synchronized ControllerClientTransport create() {
      if (closed) throw new IllegalStateException("Factory closed");
      var wire = new NettyControllerClientTransport(this);
      transports.add(wire);
      return wire;
    }

    public synchronized void close() {
      if (closed) return;
      closed = true;
      for (var wire : List.copyOf(transports)) wire.close();
      decode.shutdown();
      loops.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    }
  }

  private final Factory factory;
  private volatile Channel channel;
  private volatile boolean closed;

  private NettyControllerClientTransport(Factory factory) {
    this.factory = factory;
  }

  /** {@inheritDoc} Uses a 1-second TCP connect timeout. */
  public synchronized CompletableFuture<Void> connect(
      InetSocketAddress address, Consumer<Frame> receive, Consumer<Throwable> failure) {
    if (closed) return CompletableFuture.failedFuture(new IOException("Transport closed"));
    var ready = new CompletableFuture<Void>();
    new Bootstrap()
        .group(factory.loops)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
        .handler(
            new ChannelInitializer<SocketChannel>() {
              protected void initChannel(SocketChannel socket) {
                socket
                    .pipeline()
                    .addLast(
                        factory.broker == null
                            ? new QuorumFrameDecoder(factory.config, factory.inbound, factory.inbound, factory.clock)
                            : new BoundedFrameDecoder(70, factory.wireLimits.maxFrameBytes(), factory.inbound, factory.clock));
                socket
                    .pipeline()
                    .addLast(
                        new ChannelInboundHandlerAdapter() {
                          private int pendingDecode;
                          private boolean disconnected;

                          private synchronized void decodingStarted() { pendingDecode++; }

                          private void decodingFinished() {
                            boolean notify;
                            synchronized (this) {
                              pendingDecode--;
                              notify = disconnected && pendingDecode == 0;
                            }
                            if (notify && !closed) failure.accept(new IOException("Controller disconnected"));
                          }

                          public void channelActive(ChannelHandlerContext context) {
                            synchronized (NettyControllerClientTransport.this) {
                              if (closed) {
                                context.close();
                                ready.completeExceptionally(new IOException("Transport closed"));
                              } else {
                                channel = context.channel();
                                ready.complete(null);
                              }
                            }
                          }

                          public void channelRead(ChannelHandlerContext context, Object message) {
                            final byte[] bytes;
                            final AutoCloseable raw;
                            if (message instanceof QuorumFrameDecoder.OwnedFrame voterFrame) {
                              bytes = voterFrame.bytes(); raw = voterFrame;
                            } else if (factory.broker != null && message instanceof BoundedFrameDecoder.OwnedFrame brokerFrame) {
                              bytes = brokerFrame.bytes(); raw = brokerFrame;
                            } else {
                              io.netty.util.ReferenceCountUtil.release(message);
                              context.close();
                              return;
                            }
                            decodingStarted();
                            try {
                              factory.decode.execute(
                                  () -> {
                                    try (raw) {
                                      // Preflight bounds the decoded size, so the budget is
                                      // reserved before any payload is materialized.
                                      long allocation = QuorumCodec.preflight(bytes, factory.wireLimits);
                                      try (var lease =
                                          factory
                                              .inbound
                                              .reserve(allocation)
                                              .orElseThrow(
                                                  () ->
                                                      new IOException("Decode budget exhausted"))) {
                                        var frame = QuorumCodec.decode(bytes, factory.wireLimits);
                                        if (factory.broker != null && (frame.version() != 2 || !frame.response()
                                            || frame.senderRole() != BrokerControlProtocol.SenderRole.VOTER
                                            || frame.operation() < 106 || !frame.clusterId().equals(factory.broker.clusterId())))
                                          throw new IOException("Invalid broker control reply identity");
                                        receive.accept(frame);
                                      }
                                    } catch (Exception error) {
                                      failure.accept(error);
                                      context.close();
                                    } finally {
                                      decodingFinished();
                                    }
                                  });
                            } catch (RejectedExecutionException error) {
                              try { raw.close(); } catch (Exception close) { error.addSuppressed(close); }
                              failure.accept(error);
                              context.close();
                              decodingFinished();
                            }
                          }

                          public void channelInactive(ChannelHandlerContext context) {
                            boolean notify;
                            synchronized (this) {
                              disconnected = true;
                              notify = pendingDecode == 0;
                            }
                            if (notify && !closed) failure.accept(new IOException("Controller disconnected"));
                          }

                          public void exceptionCaught(
                              ChannelHandlerContext context, Throwable error) {
                            failure.accept(error);
                            context.close();
                          }
                        });
              }
            })
        .connect(address)
        .addListener(
            result -> {
              if (!result.isSuccess()) ready.completeExceptionally(result.cause());
            });
    return ready;
  }

  /**
   * {@inheritDoc} Fails without writing if the shared outbound budget has no room or the encoded
   * frame exceeds 4 KiB.
   */
  public CompletableFuture<Void> send(Frame frame) {
    if (factory.broker != null && (frame.version() != 2 || frame.response()
        || frame.senderRole() != BrokerControlProtocol.SenderRole.BROKER
        || frame.senderId() != factory.broker.brokerId() || frame.operation() < 106
        || !frame.clusterId().equals(factory.broker.clusterId()) || !Arrays.equals(frame.voterHash(), new byte[32])))
      return CompletableFuture.failedFuture(new IOException("Invalid broker control request identity"));
    var current = channel;
    if (closed || current == null || !current.isActive())
      return CompletableFuture.failedFuture(new IOException("Controller not connected"));
    // Admin requests have no batches or chunks; 4 KiB covers their validated v1 envelope.
    var lease = factory.outbound.reserve(4096).orElse(null);
    if (lease == null)
      return CompletableFuture.failedFuture(new IOException("Outbound budget exhausted"));
    try {
      byte[] bytes = QuorumCodec.encode(frame);
      if (bytes.length > 4096) throw new IOException("Admin request too large");
      var done = new CompletableFuture<Void>();
      current
          .writeAndFlush(Unpooled.wrappedBuffer(bytes))
          .addListener(
              result -> {
                lease.close();
                if (result.isSuccess()) done.complete(null);
                else done.completeExceptionally(result.cause());
              });
      return done;
    } catch (Exception error) {
      lease.close();
      return CompletableFuture.failedFuture(error);
    }
  }

  public synchronized void close() {
    if (closed) return;
    closed = true;
    if (channel != null) channel.close();
    factory.transports.remove(this);
  }
}
