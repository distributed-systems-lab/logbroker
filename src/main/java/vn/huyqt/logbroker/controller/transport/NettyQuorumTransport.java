package vn.huyqt.logbroker.controller.transport;

import io.netty.bootstrap.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/** Netty owns sockets only. Validation, DTO ownership and core dispatch are bounded separately. */
public final class NettyQuorumTransport implements QuorumTransport {
  public static final class TransportException extends IOException {
    private final long connectionId;
    private final int peer;

    TransportException(long connectionId, int peer, Throwable cause) {
      super("Quorum connection " + connectionId + " peer " + peer, cause);
      this.connectionId = connectionId;
      this.peer = peer;
    }

    public long connectionId() {
      return connectionId;
    }

    public int peer() {
      return peer;
    }
  }

  private final ControllerConfig config;
  private final DeadlineScheduler clock;
  private final ResourceBudget inboundControl,
      inboundShared,
      outboundControl,
      outboundShared,
      connectionBudget = new ResourceBudget(64);
  private final ResourceBudget peerConnections = new ResourceBudget(8);
  private final Validation validation = new Validation();
  private final AtomicLong connectionIds = new AtomicLong();
  private final Map<Long, Connection> routes = new ConcurrentHashMap<>();
  private final Map<Integer, CompletableFuture<Channel>> peers = new ConcurrentHashMap<>();
  private MultiThreadIoEventLoopGroup boss, loops;
  private Channel listener;
  private Consumer<Inbound> receiver;
  private Consumer<Throwable> failure;
  private volatile boolean closed;
  private CompletableFuture<Void> closing;

  public NettyQuorumTransport(ControllerConfig config, DeadlineScheduler clock) {
    this.config = config;
    this.clock = clock;
    long inControl = Math.min(8L * 1024 * 1024, config.inboundBytes() / 8),
        outControl = Math.min(8L * 1024 * 1024, config.outboundBytes() / 8);
    inboundControl = new ResourceBudget(inControl);
    inboundShared = new ResourceBudget(config.inboundBytes() - inControl);
    outboundControl = new ResourceBudget(outControl);
    outboundShared = new ResourceBudget(config.outboundBytes() - outControl);
  }

  @Override
  public synchronized InetSocketAddress start(
      InetSocketAddress bind, Consumer<Inbound> receiver, Consumer<Throwable> failure)
      throws IOException {
    if (listener != null || closed)
      throw new IllegalStateException("Transport already started or closed");
    this.receiver = receiver;
    this.failure = failure;
    boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    loops = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    try {
      var bound =
          new ServerBootstrap()
              .group(boss, loops)
              .channel(NioServerSocketChannel.class)
              .childHandler(initializer(-2))
              .bind(bind)
              .syncUninterruptibly();
      if (!bound.isSuccess()) throw new IOException("Cannot bind quorum listener", bound.cause());
      listener = bound.channel();
      return (InetSocketAddress) listener.localAddress();
    } catch (RuntimeException | IOException error) {
      closeAsync();
      throw new IOException("Cannot start quorum transport", error);
    }
  }

  private ChannelInitializer<SocketChannel> initializer(int expectedPeer) {
    return new ChannelInitializer<>() {
      @Override
      protected void initChannel(SocketChannel channel) {
        channel
            .pipeline()
            .addLast(new QuorumFrameDecoder(config, inboundControl, inboundShared, clock));
        channel.pipeline().addLast(new Connection(expectedPeer));
      }
    };
  }

  @Override
  public CompletableFuture<Void> send(int peerId, Frame frame) {
    if (closed || loops == null || frame.response())
      return CompletableFuture.failedFuture(new IOException("Quorum transport unavailable"));
    final ClusterIdentity.Voter voter;
    try {
      voter = config.identity().voter(peerId);
    } catch (IllegalArgumentException error) {
      return CompletableFuture.failedFuture(error);
    }
    var ready =
        peers.computeIfAbsent(
            peerId,
            ignored -> {
              var connected = new CompletableFuture<Channel>();
              new Bootstrap()
                  .group(loops)
                  .channel(NioSocketChannel.class)
                  .option(
                      ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) config.rpcTimeout().toMillis())
                  .handler(initializer(peerId))
                  .connect(new InetSocketAddress(voter.host(), voter.port()))
                  .addListener(
                      result -> {
                        if (result.isSuccess()) {
                          var channel = ((ChannelFuture) result).channel();
                          if (closed) {
                            channel.close();
                            connected.completeExceptionally(new IOException("Transport closed"));
                          } else {
                            var connection = channel.pipeline().get(Connection.class);
                            connection.activated.whenComplete(
                                (active, error) -> {
                                  if (error == null) connected.complete(active);
                                  else connected.completeExceptionally(error);
                                });
                          }
                        } else connected.completeExceptionally(result.cause());
                      });
              return connected;
            });
    return ready
        .thenCompose(
            channel -> {
              var connection = channel.pipeline().get(Connection.class);
              return connection == null
                  ? CompletableFuture.failedFuture(new IOException("Connection removed"))
                  : connection.writeRequest(frame);
            })
        .whenComplete(
            (ignored, error) -> {
              if (error != null) peers.remove(peerId, ready);
            });
  }

  @Override
  public CompletableFuture<Void> reply(ReplyRoute route, Frame frame) {
    var connection = routes.get(route.connectionId());
    if (connection == null || route.requestId() != frame.requestId() || !frame.response())
      return CompletableFuture.failedFuture(new IOException("Reply route closed or mismatched"));
    return connection.writeReply(frame);
  }

  @Override
  public synchronized void stopAccepting() {
    if (listener != null) listener.close();
  }

  @Override
  public synchronized CompletableFuture<Void> closeAsync() {
    if (closing != null) return closing;
    closed = true;
    stopAccepting();
    closing =
        CompletableFuture.runAsync(
            () -> {
              for (var connection : List.copyOf(routes.values()))
                connection.context.close().syncUninterruptibly();
              validation.close();
              if (loops != null)
                loops.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
              if (boss != null)
                boss.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
              peers.clear();
            });
    return closing;
  }

  public long inboundUsed() {
    return inboundControl.used() + inboundShared.used();
  }

  public long outboundUsed() {
    return outboundControl.used() + outboundShared.used();
  }

  /**
   * Covers transient storage decoding/DTOs before a reply can acquire its own encode/write lease.
   */
  public ResourceBudget.Lease reserveReadMemory(int bytes) {
    return outboundShared.reserve(12L * bytes + 1024).orElse(null);
  }

  boolean validationTask(boolean control, Runnable action) {
    return validation.execute(control, action);
  }

  public long connectionsUsed() {
    return connectionBudget.used();
  }

  private ResourceBudget.Lease reserve(
      ResourceBudget control, ResourceBudget shared, long amount, boolean peerControl) {
    var lease = (peerControl ? control : shared).reserve(amount).orElse(null);
    return lease == null && peerControl ? shared.reserve(amount).orElse(null) : lease;
  }

  private boolean control(Frame frame, int length, boolean peer) {
    return peer && frame.operation() != 105 && frame.operation() <= 106 && length <= 64 * 1024;
  }

  private synchronized ResourceBudget.Lease admitConnection() {
    var lease = connectionBudget.reserve(1).orElse(null);
    if (lease != null) return lease;
    var victim =
        routes.values().stream()
            .filter(c -> c.peer < 0 && !c.evicted)
            .min(Comparator.comparingLong(c -> c.id))
            .orElse(null);
    if (victim == null) return null;
    victim.evicted = true;
    routes.remove(victim.id, victim);
    victim.connectionLease.close();
    victim.context.close();
    return connectionBudget.reserve(1).orElse(null);
  }

  private final class Connection extends ChannelInboundHandlerAdapter {
    private final long id = connectionIds.incrementAndGet();
    private final int expectedPeer;
    private volatile int peer = -2;
    private volatile boolean evicted;
    private ChannelHandlerContext context;
    private ResourceBudget.Lease connectionLease, peerLease;
    private DeadlineScheduler.Ticket handshake;
    private final ArrayDeque<QuorumFrameDecoder.OwnedFrame> received = new ArrayDeque<>();
    private final Semaphore incomingSlots = new Semaphore(32), outgoingSlots = new Semaphore(32);
    private final Map<Long, Inbound> requests = new ConcurrentHashMap<>();
    private final Map<Long, Rpc> rpcs = new ConcurrentHashMap<>();
    private boolean decoding;
    private final CompletableFuture<Channel> activated = new CompletableFuture<>();

    private record Rpc(short operation, DeadlineScheduler.Ticket timeout) {}

    Connection(int expectedPeer) {
      this.expectedPeer = expectedPeer;
      if (expectedPeer >= 0) peer = expectedPeer;
    }

    @Override
    public void channelActive(ChannelHandlerContext context) {
      this.context = context;
      connectionLease = admitConnection();
      if (expectedPeer >= 0) peerLease = peerConnections.reserve(1).orElse(null);
      if (connectionLease == null || expectedPeer >= 0 && peerLease == null || closed) {
        activated.completeExceptionally(new IOException("Connection admission unavailable"));
        context.close();
        return;
      }
      routes.put(id, this);
      if (expectedPeer < 0)
        handshake =
            clock.schedule(
                clock.nanoTime() + config.rpcTimeout().toNanos(),
                () -> {
                  if (peer == -2) context.close();
                });
      context.fireChannelActive();
      activated.complete(context.channel());
    }

    @Override
    public synchronized void channelRead(ChannelHandlerContext context, Object message) {
      if (!(message instanceof QuorumFrameDecoder.OwnedFrame frame)) {
        context.close();
        return;
      }
      if (!incomingSlots.tryAcquire()) {
        frame.close();
        context.close();
        return;
      }
      received.addLast(frame);
      scheduleDecode();
    }

    private synchronized void scheduleDecode() {
      if (decoding || received.isEmpty() || !context.channel().isActive()) return;
      var raw = received.removeFirst();
      decoding = true;
      if (!validation.execute(
          raw.control(),
          () -> {
            try {
              decode(raw);
            } finally {
              synchronized (this) {
                decoding = false;
                scheduleDecode();
              }
            }
          })) {
        decoding = false;
        raw.close();
        incomingSlots.release();
        context.close();
      }
    }

    private void decode(QuorumFrameDecoder.OwnedFrame raw) {
      ResourceBudget.Lease decoded = null;
      boolean transferred = false;
      try {
        long charge = QuorumCodec.preflight(raw.bytes(), config);
        decoded = reserve(inboundControl, inboundShared, charge, raw.control());
        if (decoded == null) {
          context.close();
          return;
        }
        Frame frame = QuorumCodec.decode(raw.bytes(), config);
        synchronized (this) {
          if (!context.channel().isActive() || evicted || closed) return;
          if (expectedPeer >= 0) {
            if (!frame.response() || frame.senderId() != expectedPeer) {
              context.close();
              return;
            }
            var rpc = rpcs.get(frame.requestId());
            if (rpc == null || rpc.operation() != frame.operation()) return;
            if (rpcs.remove(frame.requestId(), rpc)) {
              rpc.timeout().cancel();
              outgoingSlots.release();
            }
          } else {
            if (frame.response() || peer != -2 && peer != frame.senderId()) {
              context.close();
              return;
            }
            if (peer == -2 && frame.senderId() >= 0) {
              peerLease = peerConnections.reserve(1).orElse(null);
              if (peerLease == null) {
                context.close();
                return;
              }
            }
            peer = frame.senderId();
            if (handshake != null) {
              handshake.cancel();
              handshake = null;
            }
            if (requests.containsKey(frame.requestId())) {
              context.close();
              return;
            }
          }
          final var ownedDecoded = decoded;
          var inbound =
              new Inbound(
                      new ReplyRoute(id, frame.requestId()),
                      frame,
                      () -> {
                        ownedDecoded.close();
                        raw.close();
                        incomingSlots.release();
                      },
                      () -> context.channel().isActive())
                  .rejectWith(() -> context.close());
          if (!frame.response()) requests.put(frame.requestId(), inbound.retain());
          transferred = true;
          try {
            receiver.accept(inbound);
          } catch (RuntimeException error) {
            inbound.close();
            context.close();
            notifyFailure(error);
          }
        }
      } catch (Exception error) {
        context.close();
        notifyFailure(error);
      } finally {
        if (!transferred) {
          if (decoded != null) decoded.close();
          raw.close();
          incomingSlots.release();
        }
      }
    }

    CompletableFuture<Void> writeRequest(Frame frame) {
      if (!context.channel().isActive() || closed || !outgoingSlots.tryAcquire())
        return CompletableFuture.failedFuture(
            new IOException("Peer connection unavailable or full"));
      var ticket =
          clock.schedule(
              clock.nanoTime() + config.rpcTimeout().toNanos(),
              () -> {
                var removed = rpcs.remove(frame.requestId());
                if (removed != null) outgoingSlots.release();
              });
      var rpc = new Rpc(frame.operation(), ticket);
      if (rpcs.putIfAbsent(frame.requestId(), rpc) != null) {
        ticket.cancel();
        outgoingSlots.release();
        return CompletableFuture.failedFuture(new IOException("Duplicate outgoing request ID"));
      }
      return write(frame)
          .whenComplete(
              (ignored, error) -> {
                if (error != null && rpcs.remove(frame.requestId(), rpc)) {
                  ticket.cancel();
                  outgoingSlots.release();
                }
              });
    }

    CompletableFuture<Void> writeReply(Frame frame) {
      var inbound = requests.get(frame.requestId());
      if (inbound == null || inbound.frame().operation() != frame.operation())
        return CompletableFuture.failedFuture(new IOException("Unknown reply request"));
      return write(frame)
          .whenComplete(
              (ignored, error) -> {
                var owned = requests.remove(frame.requestId());
                if (owned != null) owned.close();
              });
    }

    private CompletableFuture<Void> write(Frame frame) {
      var result = new CompletableFuture<Void>();
      final long size, charge;
      try {
        size = QuorumCodec.encodedSize(frame);
        charge = QuorumCodec.outboundCharge(frame);
      } catch (RuntimeException error) {
        return CompletableFuture.failedFuture(error);
      }
      if (size > config.maxFrameBytes() + 4L)
        return CompletableFuture.failedFuture(new IOException("Outbound frame exceeds limit"));
      boolean isControl = control(frame, (int) size, peer >= 0);
      var lease = reserve(outboundControl, outboundShared, charge, isControl);
      if (lease == null)
        return CompletableFuture.failedFuture(
            new IOException("Outbound ownership budget exhausted"));
      if (!validation.execute(
          isControl,
          () -> {
            boolean handed = false;
            try {
              if (!context.channel().isActive() || evicted || closed) {
                result.completeExceptionally(new IOException("Connection closed"));
                return;
              }
              byte[] bytes = QuorumCodec.encode(frame);
              if (bytes.length > config.maxFrameBytes() + 4)
                throw new IOException("Outbound frame exceeds limit");
              if (bytes.length != size) throw new IOException("Outbound sizing mismatch");
              context
                  .writeAndFlush(Unpooled.wrappedBuffer(bytes))
                  .addListener(
                      done -> {
                        lease.close();
                        if (done.isSuccess()) result.complete(null);
                        else {
                          result.completeExceptionally(done.cause());
                          context.close();
                        }
                      });
              handed = true;
            } catch (Exception error) {
              result.completeExceptionally(error);
              context.close();
            } finally {
              if (!handed) lease.close();
            }
          })) {
        lease.close();
        result.completeExceptionally(new IOException("Validation admission exhausted"));
      }
      return result;
    }

    private void notifyFailure(Throwable error) {
      if (!closed && failure != null) failure.accept(new TransportException(id, peer, error));
    }

    @Override
    public synchronized void channelInactive(ChannelHandlerContext context) {
      activated.completeExceptionally(new IOException("Connection closed before activation"));
      routes.remove(id, this);
      if (expectedPeer >= 0) peers.remove(expectedPeer);
      if (connectionLease != null) connectionLease.close();
      if (peerLease != null) peerLease.close();
      if (handshake != null) handshake.cancel();
      while (!received.isEmpty()) {
        received.removeFirst().close();
        incomingSlots.release();
      }
      for (var request : List.copyOf(requests.keySet())) {
        var owned = requests.remove(request);
        if (owned != null) owned.close();
      }
      for (var request : List.copyOf(rpcs.keySet())) {
        var rpc = rpcs.remove(request);
        if (rpc != null) {
          rpc.timeout().cancel();
          outgoingSlots.release();
        }
      }
      notifyFailure(new IOException("Connection closed"));
      context.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable error) {
      notifyFailure(error);
      context.close();
    }
  }

  /** A 256-task bound with 64 admission slots unavailable to admin/snapshot validation. */
  private static final class Validation implements AutoCloseable {
    private final Semaphore all = new Semaphore(256), shared = new Semaphore(192);
    private final AtomicLong sequence = new AtomicLong();
    private final ThreadPoolExecutor pool =
        new ThreadPoolExecutor(
            2,
            2,
            0,
            TimeUnit.MILLISECONDS,
            new PriorityBlockingQueue<>(),
            r -> new Thread(r, "quorum-validation"));

    private final class Job implements Runnable, Comparable<Job> {
      final boolean control;
      final long order = sequence.incrementAndGet();
      final Runnable action;

      Job(boolean control, Runnable action) {
        this.control = control;
        this.action = action;
      }

      @Override
      public int compareTo(Job other) {
        int priority = Boolean.compare(other.control, control);
        return priority == 0 ? Long.compare(order, other.order) : priority;
      }

      @Override
      public void run() {
        try {
          action.run();
        } finally {
          all.release();
          if (!control) shared.release();
        }
      }
    }

    boolean execute(boolean control, Runnable action) {
      if (!control && !shared.tryAcquire()) return false;
      if (!all.tryAcquire()) {
        if (!control) shared.release();
        return false;
      }
      try {
        pool.execute(new Job(control, action));
        return true;
      } catch (RejectedExecutionException error) {
        all.release();
        if (!control) shared.release();
        return false;
      }
    }

    @Override
    public void close() {
      pool.shutdown();
      try {
        if (!pool.awaitTermination(30, TimeUnit.SECONDS))
          throw new IllegalStateException("Validation worker did not stop");
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(error);
      }
    }
  }
}
