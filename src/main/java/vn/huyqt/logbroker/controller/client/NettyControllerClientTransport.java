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

/** Shared factory budgets and workers; each attempt owns one channel and at most one RPC. */
public final class NettyControllerClientTransport implements ControllerClientTransport {
  public static final class Factory implements ControllerClientTransport.Factory {
    private final ControllerConfig config;
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

    public Factory(ClusterIdentity identity, DeadlineScheduler clock) {
      config = ControllerConfig.defaults(identity);
      this.clock = clock;
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
                        new QuorumFrameDecoder(
                            factory.config, factory.inbound, factory.inbound, factory.clock));
                socket
                    .pipeline()
                    .addLast(
                        new ChannelInboundHandlerAdapter() {
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
                            if (!(message instanceof QuorumFrameDecoder.OwnedFrame raw)) {
                              io.netty.util.ReferenceCountUtil.release(message);
                              context.close();
                              return;
                            }
                            try {
                              factory.decode.execute(
                                  () -> {
                                    try (raw) {
                                      long bytes =
                                          QuorumCodec.preflight(raw.bytes(), factory.config);
                                      try (var lease =
                                          factory
                                              .inbound
                                              .reserve(bytes)
                                              .orElseThrow(
                                                  () ->
                                                      new IOException("Decode budget exhausted"))) {
                                        receive.accept(
                                            QuorumCodec.decode(raw.bytes(), factory.config));
                                      }
                                    } catch (Exception error) {
                                      failure.accept(error);
                                      context.close();
                                    }
                                  });
                            } catch (RejectedExecutionException error) {
                              raw.close();
                              failure.accept(error);
                              context.close();
                            }
                          }

                          public void channelInactive(ChannelHandlerContext context) {
                            if (!closed) failure.accept(new IOException("Controller disconnected"));
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

  public CompletableFuture<Void> send(Frame frame) {
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
