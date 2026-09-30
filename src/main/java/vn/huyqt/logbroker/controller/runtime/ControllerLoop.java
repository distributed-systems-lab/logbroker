package vn.huyqt.logbroker.controller.runtime;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent;

/**
 * One state owner with separately reserved control and disk completion capacity.
 *
 * <p>A single dispatch thread (or the caller of {@link #drain} when unthreaded) hands every event
 * to the handler, so consensus state needs no locking. Events are held in three bounded queues:
 * disk completions, control events and general (admin and snapshot) events. Completion capacity is
 * reserved through {@link #reserveCompletion} before disk work is admitted, so a finished disk
 * operation can always be delivered. Dispatch order is completions first, then control, with
 * general events getting a turn after eight consecutive control events.
 *
 * <p>Submission and reservation are thread-safe; queue state is guarded by this instance's monitor.
 */
public final class ControllerLoop implements AutoCloseable {
  /** Admission class of a submitted event. {@code ADMIN} and {@code SNAPSHOT} share one queue. */
  public enum Priority {
    CONTROL,
    ADMIN,
    SNAPSHOT
  }

  private final int generalCap, controlCap, completionCap;
  private final ArrayDeque<QuorumEvent> general = new ArrayDeque<>(),
      control = new ArrayDeque<>(),
      completions = new ArrayDeque<>();
  private final Consumer<QuorumEvent> handler;
  private final Thread worker;
  private int reserved, controlTurns;
  private boolean closed;

  /**
   * @param total overall event capacity; the general queue gets what control and completion
   *     slots leave
   * @param controls capacity of the control queue
   * @param completionSlots number of disk completions that may be reserved at once
   * @param threaded whether to start a dedicated dispatch thread; if false, the caller dispatches
   *     with {@link #drain}
   */
  public ControllerLoop(
      int total,
      int controls,
      int completionSlots,
      Consumer<QuorumEvent> handler,
      boolean threaded) {
    if (controls < 1 || completionSlots < 1 || total <= controls + completionSlots)
      throw new IllegalArgumentException("Invalid loop capacities");
    generalCap = total - controls - completionSlots;
    controlCap = controls;
    completionCap = completionSlots;
    this.handler = handler;
    worker = threaded ? new Thread(this::run, "controller-loop") : null;
    if (worker != null) worker.start();
  }

  /**
   * Queues an event for dispatch.
   *
   * @return false if the loop is closed or the queue for {@code priority} is full; the event is
   *     then not queued and the caller owns the rejection
   */
  public synchronized boolean submit(QuorumEvent event, Priority priority) {
    if (closed) return false;
    var queue = priority == Priority.CONTROL ? control : general;
    // At most one Tick is queued: a newer one replaces it, so ticks cannot fill the control queue.
    if (priority == Priority.CONTROL && event instanceof QuorumEvent.Tick) {
      for (var iterator = control.iterator(); iterator.hasNext(); )
        if (iterator.next() instanceof QuorumEvent.Tick) {
          iterator.remove();
          control.addLast(event);
          notifyAll();
          return true;
        }
    }
    int limit = priority == Priority.CONTROL ? controlCap : generalCap;
    if (queue.size() >= limit) return false;
    queue.addLast(event);
    notifyAll();
    return true;
  }

  /**
   * Reserves one completion slot. The slot is freed when the loop dispatches the completion, or by
   * closing the ticket without completing it.
   *
   * @return the ticket, or null if the loop is closed or all completion slots are reserved
   */
  public synchronized CompletionTicket reserveCompletion() {
    if (closed || reserved >= completionCap) return null;
    reserved++;
    return new CompletionTicket();
  }

  /** A reserved completion slot; used exactly once, by either {@link #complete} or close. */
  public final class CompletionTicket implements AutoCloseable {
    private final AtomicBoolean published = new AtomicBoolean();

    /**
     * Queues {@code event} in the reserved slot; this cannot fail for lack of queue capacity.
     *
     * @throws IllegalStateException if the ticket was already completed or closed
     */
    public void complete(QuorumEvent event) {
      if (!published.compareAndSet(false, true))
        throw new IllegalStateException("Completion already released");
      synchronized (ControllerLoop.this) {
        completions.addLast(event);
        ControllerLoop.this.notifyAll();
      }
    }

    /** Releases the slot if it was never completed; otherwise does nothing. */
    @Override
    public void close() {
      if (published.compareAndSet(false, true))
        synchronized (ControllerLoop.this) {
          reserved--;
          ControllerLoop.this.notifyAll();
        }
    }
  }

  private synchronized QuorumEvent next() {
    if (!completions.isEmpty()) {
      reserved--;
      return completions.removeFirst();
    }
    if (!general.isEmpty() && (control.isEmpty() || controlTurns >= 8)) {
      controlTurns = 0;
      return general.removeFirst();
    }
    if (!control.isEmpty()) {
      controlTurns++;
      return control.removeFirst();
    }
    return general.pollFirst();
  }

  /**
   * Dispatches queued events on the calling thread until all queues are empty. Only for loops
   * created unthreaded.
   *
   * @throws IllegalStateException if this loop has its own dispatch thread
   */
  public void drain() {
    if (worker != null) throw new IllegalStateException("Threaded loop owns dispatch");
    QuorumEvent event;
    while ((event = next()) != null) handler.accept(event);
  }

  private void run() {
    while (true) {
      QuorumEvent event = next();
      if (event != null) {
        handler.accept(event);
        continue;
      }
      synchronized (this) {
        if (closed) return;
        try {
          if (queued() == 0) wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  public synchronized int queued() {
    return general.size() + control.size() + completions.size();
  }

  /** Completion slots currently reserved, including completions queued but not yet dispatched. */
  public synchronized int reservedCompletions() {
    return reserved;
  }

  /**
   * Stops accepting events and waits up to 30 seconds for the dispatch thread to exit. Events
   * already queued are still dispatched before it exits.
   *
   * @throws IllegalStateException if any completion slot is still reserved, so disk work must be
   *     drained first, or if the dispatch thread does not stop in time
   */
  @Override
  public void close() {
    synchronized (this) {
      if (reserved != 0)
        throw new IllegalStateException("Disk I/O still owns completion reservations");
      closed = true;
      notifyAll();
    }
    if (worker != null && Thread.currentThread() != worker)
      try {
        worker.join(30_000);
        if (worker.isAlive()) throw new IllegalStateException("Controller loop did not stop");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
  }
}
