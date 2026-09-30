package vn.huyqt.logbroker.controller.runtime;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent;

/** One state owner with separately reserved control and disk completion capacity. */
public final class ControllerLoop implements AutoCloseable {
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

  public synchronized boolean submit(QuorumEvent event, Priority priority) {
    if (closed) return false;
    var queue = priority == Priority.CONTROL ? control : general;
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

  public synchronized CompletionTicket reserveCompletion() {
    if (closed || reserved >= completionCap) return null;
    reserved++;
    return new CompletionTicket();
  }

  public final class CompletionTicket implements AutoCloseable {
    private final AtomicBoolean published = new AtomicBoolean();

    public void complete(QuorumEvent event) {
      if (!published.compareAndSet(false, true))
        throw new IllegalStateException("Completion already released");
      synchronized (ControllerLoop.this) {
        completions.addLast(event);
        ControllerLoop.this.notifyAll();
      }
    }

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

  public synchronized int reservedCompletions() {
    return reserved;
  }

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
