package vn.huyqt.logbroker.controller.client;

import static vn.huyqt.logbroker.controller.client.ControllerClientException.Outcome.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/**
 * One connection and correlation per attempt; one scheduled action per invocation. The injected
 * scheduler remains caller-owned; the transport factory is client-owned.
 */
public final class ControllerClient implements AutoCloseable {
  private final ClusterIdentity identity;
  private final List<InetSocketAddress> bootstrap;
  private final DeadlineScheduler clock;
  private final ControllerClientTransport.Factory factory;
  private final Map<Long, Call<?>> active = new HashMap<>();
  private final Semaphore capacity = new Semaphore(1024);
  private final Random jitter = new Random(0);
  private long ids;
  private int nextBootstrap;
  private boolean closed;
  private final ThreadPoolExecutor responses =
      new ThreadPoolExecutor(
          2,
          2,
          0,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(1024),
          r -> new Thread(r, "controller-client-response"));

  public ControllerClient(
      ClusterIdentity identity,
      List<InetSocketAddress> bootstrap,
      DeadlineScheduler clock,
      ControllerClientTransport.Factory factory) {
    this.identity = Objects.requireNonNull(identity);
    this.bootstrap = List.copyOf(bootstrap);
    if (bootstrap.isEmpty()) throw new IllegalArgumentException("Empty bootstrap");
    this.clock = Objects.requireNonNull(clock);
    this.factory = Objects.requireNonNull(factory);
  }

  public CompletableFuture<UUID> createTopic(String name, int partitions) {
    new TopicCreated(new UUID(0, 1), name, partitions);
    return invoke(
        -1,
        remaining -> new CreateTopic(name, partitions, remaining),
        reply -> ((CreateTopicReply) reply).topicId());
  }

  public CompletableFuture<MetadataView> metadata() {
    return invoke(
        -1,
        ReadMetadata::new,
        reply -> {
          var view = ((MetadataReply) reply).view();
          if (view.consistency() != Consistency.LINEARIZABLE)
            throw new IllegalArgumentException("Nonlinearizable reply");
          return view;
        });
  }

  public CompletableFuture<MetadataView> localMetadata(int nodeId) {
    identity.voter(nodeId);
    return invoke(
        nodeId, ignored -> new ReadLocalMetadata(), reply -> ((MetadataReply) reply).view());
  }

  public CompletableFuture<QuorumStatus> describe(int nodeId) {
    identity.voter(nodeId);
    return invoke(
        nodeId, ignored -> new DescribeQuorum(), reply -> ((DescribeQuorumReply) reply).status());
  }

  private synchronized <T> CompletableFuture<T> invoke(
      int pinned, Function<Integer, Request> request, Function<Reply, T> result) {
    if (closed || !capacity.tryAcquire())
      return CompletableFuture.failedFuture(
          new ControllerClientException(
              closed ? QuorumError.NODE_UNAVAILABLE : QuorumError.OVERLOADED,
              NOT_SENT,
              "Client unavailable"));
    var call = new Call<T>(++ids, pinned, clock.nanoTime() + 30_000_000_000L, request, result);
    active.put(call.id, call);
    call.future.whenComplete(
        (value, error) -> {
          if (call.future.isCancelled())
            synchronized (ControllerClient.this) {
              finish(call, null, new CancellationException());
            }
        });
    attempt(call);
    return call.future;
  }

  private InetSocketAddress address(int id) {
    var voter = identity.voter(id);
    return new InetSocketAddress(voter.host(), voter.port());
  }

  private synchronized void attempt(Call<?> call) {
    if (!active.containsKey(call.id)) return;
    if (clock.nanoTime() >= call.deadline) {
      timeout(call);
      return;
    }
    clearAttempt(call);
    call.requestId = ++ids;
    long requestId = call.requestId;
    var wire = factory.create();
    call.wire = wire;
    InetSocketAddress target =
        call.pinned >= 0
            ? address(call.pinned)
            : call.hint >= 0
                ? address(call.hint)
                : bootstrap.get(nextBootstrap++ % bootstrap.size());
    call.hint = -1;
    arm(
        call,
        Math.min(call.deadline, clock.nanoTime() + 1_000_000_000L),
        () -> retry(call, requestId));
    try {
      wire.connect(
              target, frame -> receive(call, requestId, frame), error -> retry(call, requestId))
          .whenComplete(
              (ignored, error) -> {
                synchronized (ControllerClient.this) {
                  if (!current(call, requestId)) return;
                  if (error != null) {
                    retry(call, requestId);
                    return;
                  }
                  int remaining =
                      (int)
                          Math.max(
                              1, Math.min(30_000, (call.deadline - clock.nanoTime()) / 1_000_000));
                  Request request = call.request.apply(remaining);
                  var frame =
                      new Frame(
                          QuorumProtocol.operation(request),
                          false,
                          identity.clusterId(),
                          -1,
                          requestId,
                          identity.voterHash(),
                          request);
                  call.operation = frame.operation();
                  call.everPossiblySent = true;
                  try {
                    wire.send(frame)
                        .whenComplete(
                            (sent, failed) -> {
                              if (failed != null) retry(call, requestId);
                            });
                  } catch (RuntimeException failure) {
                    retry(call, requestId);
                  }
                }
              });
    } catch (RuntimeException failure) {
      retry(call, requestId);
    }
  }

  private boolean current(Call<?> call, long requestId) {
    return active.containsKey(call.id) && call.requestId == requestId && call.wire != null;
  }

  private synchronized void receive(Call<?> call, long requestId, Frame frame) {
    if (!current(call, requestId) || frame.requestId() != requestId) return;
    if (!frame.clusterId().equals(identity.clusterId())) {
      fail(call, QuorumError.CLUSTER_MISMATCH, "Wrong cluster");
      return;
    }
    if (!Arrays.equals(frame.voterHash(), identity.voterHash())) {
      fail(call, QuorumError.INCONSISTENT_VOTER_SET, "Wrong membership");
      return;
    }
    if (!frame.response()
        || frame.operation() != call.operation
        || identity.voters().stream().noneMatch(v -> v.id() == frame.senderId())
        || call.pinned >= 0 && frame.senderId() != call.pinned) {
      fail(call, QuorumError.INVALID_REQUEST, "Wrong response identity");
      return;
    }
    var reply = (Reply) frame.message();
    var error = reply.meta().error();
    if (error == QuorumError.NONE) {
      resolve(call, reply);
      return;
    }
    if (call.pinned < 0
        && (error == QuorumError.NOT_LEADER
            || error == QuorumError.STALE_EPOCH
            || error == QuorumError.OVERLOADED
            || error == QuorumError.NODE_UNAVAILABLE
            || error == QuorumError.REQUEST_TIMED_OUT)) {
      int hint = reply.meta().leaderId();
      if (identity.voters().stream().anyMatch(v -> v.id() == hint)) call.hint = hint;
      retry(call, requestId);
    } else fail(call, error, reply.meta().message());
  }

  private <T> void resolve(Call<T> call, Reply reply) {
    try {
      finish(call, call.result.apply(reply), null);
    } catch (RuntimeException error) {
      fail(call, QuorumError.INVALID_REQUEST, "Invalid response payload");
    }
  }

  private synchronized void retry(Call<?> call, long requestId) {
    if (!current(call, requestId)) return;
    clearAttempt(call);
    if (clock.nanoTime() >= call.deadline) {
      timeout(call);
      return;
    }
    long delay = Math.min(1_000_000_000L, 50_000_000L << Math.min(call.retries++, 5));
    delay = delay * 3 / 4 + (long) (jitter.nextDouble() * delay / 4);
    arm(call, Math.min(call.deadline, clock.nanoTime() + delay), () -> attempt(call));
  }

  private void arm(Call<?> call, long when, Runnable action) {
    if (call.timer != null) call.timer.cancel();
    call.timer = clock.schedule(when, action);
  }

  private void clearAttempt(Call<?> call) {
    if (call.timer != null) {
      call.timer.cancel();
      call.timer = null;
    }
    if (call.wire != null) {
      var wire = call.wire;
      call.wire = null;
      wire.close();
    }
  }

  private void timeout(Call<?> call) {
    fail(call, QuorumError.REQUEST_TIMED_OUT, "Absolute request deadline expired");
  }

  private void fail(Call<?> call, QuorumError error, String message) {
    finish(
        call,
        null,
        new ControllerClientException(error, call.everPossiblySent ? UNKNOWN : NOT_SENT, message));
  }

  private <T> void finish(Call<T> call, T value, Throwable error) {
    if (active.remove(call.id) == null) return;
    clearAttempt(call);
    responses.execute(
        () -> {
          try {
            if (error == null) call.future.complete(value);
            else call.future.completeExceptionally(error);
          } finally {
            capacity.release();
          }
        });
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    for (var call : List.copyOf(active.values()))
      fail(call, QuorumError.NODE_UNAVAILABLE, "Client closed");
    factory.close();
    responses.shutdown();
  }

  private static final class Call<T> {
    final long id, deadline;
    final int pinned;
    final Function<Integer, Request> request;
    final Function<Reply, T> result;
    final CompletableFuture<T> future = new CompletableFuture<>();
    long requestId;
    short operation;
    int hint = -1, retries;
    boolean everPossiblySent;
    ControllerClientTransport wire;
    DeadlineScheduler.Ticket timer;

    Call(
        long id,
        int pinned,
        long deadline,
        Function<Integer, Request> request,
        Function<Reply, T> result) {
      this.id = id;
      this.pinned = pinned;
      this.deadline = deadline;
      this.request = request;
      this.result = result;
    }
  }
}
