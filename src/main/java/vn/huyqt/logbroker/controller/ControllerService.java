package vn.huyqt.logbroker.controller;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/** In-process admission with a reserved response slot per accepted invocation. */
public final class ControllerService implements AutoCloseable {
  public static final class ServiceException extends RuntimeException {
    private final ReplyMeta meta;

    public ServiceException(ReplyMeta meta) {
      super(meta.error() + ": " + meta.message());
      this.meta = meta;
    }

    public ReplyMeta meta() {
      return meta;
    }
  }

  private final Predicate<QuorumEvent> admission;
  private final Executor responses;
  private final LongSupplier clock;
  private final Semaphore slots;
  private final AtomicLong ids = new AtomicLong();
  private final ConcurrentMap<Long, CompletableFuture<Reply>> pending = new ConcurrentHashMap<>();
  private final ThreadPoolExecutor owned;
  private volatile boolean closed;

  public ControllerService(
      int capacity, Predicate<QuorumEvent> admission, Executor responses, LongSupplier clock) {
    if (capacity < 1) throw new IllegalArgumentException("Invalid capacity");
    this.admission = admission;
    this.responses = responses;
    this.clock = clock;
    slots = new Semaphore(capacity);
    owned = null;
  }

  public ControllerService(int capacity, Predicate<QuorumEvent> admission, LongSupplier clock) {
    if (capacity < 1) throw new IllegalArgumentException("Invalid capacity");
    this.admission = admission;
    this.clock = clock;
    slots = new Semaphore(capacity);
    owned =
        new ThreadPoolExecutor(
            2,
            2,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(capacity),
            r -> new Thread(r, "controller-response"));
    responses = owned;
  }

  public CompletableFuture<UUID> createTopic(String name, int partitions, long deadlineNanos) {
    return invoke(new CreateTopic(name, partitions, timeout(deadlineNanos)), deadlineNanos)
        .thenApply(reply -> ((CreateTopicReply) reply).topicId());
  }

  public CompletableFuture<MetadataView> readMetadata(long deadlineNanos) {
    return invoke(new ReadMetadata(timeout(deadlineNanos)), deadlineNanos)
        .thenApply(reply -> ((MetadataReply) reply).view());
  }

  public CompletableFuture<MetadataView> readLocalMetadata() {
    return invoke(new ReadLocalMetadata(), Long.MAX_VALUE)
        .thenApply(reply -> ((MetadataReply) reply).view());
  }

  public CompletableFuture<QuorumStatus> describe() {
    return invoke(new DescribeQuorum(), Long.MAX_VALUE)
        .thenApply(reply -> ((DescribeQuorumReply) reply).status());
  }

  private int timeout(long deadline) {
    return (int) Math.max(1, Math.min(30_000, (deadline - clock.getAsLong()) / 1_000_000));
  }

  public CompletableFuture<Reply> request(Request request, long deadline) {
    return invoke(request, deadline);
  }

  private synchronized CompletableFuture<Reply> invoke(Request request, long deadline) {
    if (closed) return failed(QuorumError.NODE_UNAVAILABLE);
    if (!slots.tryAcquire()) return failed(QuorumError.OVERLOADED);
    long id = ids.incrementAndGet();
    var future = new CompletableFuture<Reply>();
    pending.put(id, future);
    if (closed || !admission.test(new QuorumEvent.Admin(id, request, deadline)))
      complete(
          id, new Failure(new ReplyMeta(QuorumError.OVERLOADED, "Admission unavailable", 0, -1)));
    return future;
  }

  private CompletableFuture<Reply> failed(QuorumError error) {
    return CompletableFuture.failedFuture(new ServiceException(new ReplyMeta(error, "", 0, -1)));
  }

  public synchronized void complete(long id, Reply reply) {
    var future = pending.remove(id);
    if (future == null) return;
    // Admission owns the queue slot until all synchronous future callbacks return.
    responses.execute(
        () -> {
          try {
            if (reply.meta().error() == QuorumError.NONE) future.complete(reply);
            else future.completeExceptionally(new ServiceException(reply.meta()));
          } finally {
            slots.release();
          }
        });
  }

  public int pendingCount() {
    return pending.size();
  }

  @Override
  public synchronized void close() {
    closed = true;
    for (long id : List.copyOf(pending.keySet()))
      complete(
          id, new Failure(new ReplyMeta(QuorumError.NODE_UNAVAILABLE, "Service closed", 0, -1)));
    if (owned != null) owned.shutdown();
  }

  public boolean awaitClosed(java.time.Duration timeout) throws InterruptedException {
    return owned == null || owned.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }
}
