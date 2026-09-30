package vn.huyqt.logbroker.controller;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

/**
 * In-process admission with a reserved response slot per accepted invocation.
 *
 * <p>Every admin request, whether made in-process or received from the network by {@link
 * ControllerNode}, passes through here. At most {@code capacity} invocations are pending; each
 * holds its slot until its future has been completed on the response executor. An accepted
 * invocation is handed to the admission predicate as a {@link QuorumEvent.Admin}; the consensus
 * core later answers it through {@link #complete}. Error replies complete the future
 * exceptionally with {@link ServiceException}.
 *
 * <p>Methods are thread-safe. Futures of accepted invocations are completed on the response
 * executor; a request rejected before admission gets an already failed future.
 */
public final class ControllerService implements AutoCloseable {
  /** Carries the reply metadata of a failed invocation, including any epoch and leader hint. */
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

  /**
   * Creates a service that completes futures on a caller-owned executor, which must accept every
   * completion task; a rejected task would leak the invocation's slot.
   *
   * @param admission offers an event to the consensus loop; {@code false} fails the invocation
   *     with {@code OVERLOADED}
   * @param clock nanosecond clock that deadlines are measured against
   */
  public ControllerService(
      int capacity, Predicate<QuorumEvent> admission, Executor responses, LongSupplier clock) {
    if (capacity < 1) throw new IllegalArgumentException("Invalid capacity");
    this.admission = admission;
    this.responses = responses;
    this.clock = clock;
    slots = new Semaphore(capacity);
    owned = null;
  }

  /**
   * Creates a service with its own two response threads, shut down by {@link #close()}.
   *
   * @see #ControllerService(int, Predicate, Executor, LongSupplier)
   */
  public ControllerService(int capacity, Predicate<QuorumEvent> admission, LongSupplier clock) {
    if (capacity < 1) throw new IllegalArgumentException("Invalid capacity");
    this.admission = admission;
    this.clock = clock;
    slots = new Semaphore(capacity);
    // A slot is released only by its completion task, so at most capacity tasks are ever queued
    // and the bounded queue cannot reject one.
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

  /**
   * Creates a topic, or returns the existing topic ID when a topic with the same name and
   * partition count exists or is pending, so a retry with identical arguments is idempotent. A
   * different partition count for an existing or pending name fails with {@code
   * TOPIC_ALREADY_EXISTS}.
   *
   * @param deadlineNanos absolute deadline on the service clock; the request carries the remaining
   *     time clamped to 1..30000 ms
   */
  public CompletableFuture<UUID> createTopic(String name, int partitions, long deadlineNanos) {
    return invoke(new CreateTopic(name, partitions, timeout(deadlineNanos)), deadlineNanos)
        .thenApply(reply -> ((CreateTopicReply) reply).topicId());
  }

  /**
   * Linearizable read: the leader answers after a read barrier appended for this request has been
   * committed and applied.
   *
   * @param deadlineNanos as for {@link #createTopic}
   */
  public CompletableFuture<MetadataView> readMetadata(long deadlineNanos) {
    return invoke(new ReadMetadata(timeout(deadlineNanos)), deadlineNanos)
        .thenApply(reply -> ((MetadataReply) reply).view());
  }

  /** Reads this node's applied catalog without a barrier; the view may be stale. */
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

  /**
   * Submits an already decoded admin request, as received from the network, under the same
   * admission bound as the typed methods. Error replies fail the future with {@link
   * ServiceException}.
   *
   * @param deadline absolute deadline on the service clock
   */
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

  /**
   * Completes invocation {@code id}. Only the first completion of an ID has an effect, so a late
   * answer after {@link #close()} or a duplicate is ignored.
   */
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

  /**
   * Rejects new requests and fails every pending invocation with {@code NODE_UNAVAILABLE}. This
   * does not withdraw work already handed to the consensus loop, so a failed create may still
   * take effect.
   */
  @Override
  public synchronized void close() {
    closed = true;
    for (long id : List.copyOf(pending.keySet()))
      complete(
          id, new Failure(new ReplyMeta(QuorumError.NODE_UNAVAILABLE, "Service closed", 0, -1)));
    if (owned != null) owned.shutdown();
  }

  /**
   * Waits for the owned response threads to finish after {@link #close()}.
   *
   * @return {@code true} if they terminated, or immediately if the executor is caller-owned
   */
  public boolean awaitClosed(java.time.Duration timeout) throws InterruptedException {
    return owned == null || owned.awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }
}
