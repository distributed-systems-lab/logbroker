package vn.huyqt.logbroker.controller.support;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/** Test-only exact-frame bridge. Isolation drops peer sessions, preserving admin observability. */
public final class ControllerFaultProxy implements AutoCloseable {
  private final int node, backend;
  private final Set<Integer> isolated;
  private final ServerSocket listener;
  private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
  private volatile boolean closed;
  private volatile boolean blockBrokers;
  private final java.util.concurrent.atomic.AtomicLong blockedBrokerFrames = new java.util.concurrent.atomic.AtomicLong();
  private final Deque<String> trace = new ConcurrentLinkedDeque<>();

  public ControllerFaultProxy(int node, int backend, Set<Integer> isolated) throws IOException {
    this.node = node;
    this.backend = backend;
    this.isolated = isolated;
    listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    Thread.startVirtualThread(
        () -> {
          while (!closed)
            try {
              accept();
            } catch (IOException error) {
              if (!closed) Thread.yield();
            }
        });
  }

  private void accept() throws IOException {
    var front = listener.accept();
    try {
      var back = new Socket("127.0.0.1", backend);
      var session = new Session(front, back);
      sessions.add(session);
      Thread.startVirtualThread(() -> copy(session, true));
      Thread.startVirtualThread(() -> copy(session, false));
    } catch (IOException error) {
      front.close();
      throw error;
    }
  }

  public int port() {
    return listener.getLocalPort();
  }

  private void copy(Session session, boolean inbound) {
    try {
      var source = new DataInputStream((inbound ? session.front : session.back).getInputStream());
      var target = new DataOutputStream((inbound ? session.back : session.front).getOutputStream());
      while (!closed) {
        int size = source.readInt();
        if (size < 69 || size > 8 * 1024 * 1024)
          throw new IOException("Invalid proxy frame length");
        byte[] bytes = source.readNBytes(size);
        if (bytes.length != size) throw new EOFException();
        if (inbound && session.sender == -2) {
          session.sender = ByteBuffer.wrap(bytes).getInt(21);
          session.broker = ByteBuffer.wrap(bytes).getShort(2) == 2 && bytes[25] == 2;
        }
        if (blocked(session)) {
          if (inbound && session.broker) blockedBrokerFrames.incrementAndGet();
          return;
        }
        if (session.broker) {
          try {
            byte[] raw = ByteBuffer.allocate(size + 4).putInt(size).put(bytes).array();
            var frame = vn.huyqt.logbroker.controller.protocol.QuorumCodec.decode(raw,
                vn.huyqt.logbroker.controller.protocol.QuorumCodec.WireLimits.observer(
                    vn.huyqt.logbroker.controller.metadata.MetadataLimits.defaults()));
            trace.addLast((inbound ? "IN " : "OUT ") + frame.operation() + " " + frame.message());
            while (trace.size() > 128) trace.pollFirst();
          } catch (IOException error) { trace.addLast("decode: " + error); }
        }
        target.writeInt(size);
        target.write(bytes);
        target.flush();
      }
    } catch (IOException ignored) {
    } finally {
      session.close();
      sessions.remove(session);
    }
  }

  private boolean blocked(Session session) {
    return session.broker ? blockBrokers
        : session.sender >= 0 && (isolated.contains(node) || isolated.contains(session.sender));
  }

  public void blockBrokers(boolean blocked) { blockBrokers = blocked; refreshIsolation(); }
  public boolean brokersBlocked() { return blockBrokers; }
  public long blockedBrokerFrames() { return blockedBrokerFrames.get(); }
  public List<String> trace() { return List.copyOf(trace); }

  public void refreshIsolation() {
    for (var session : sessions) if (blocked(session)) session.close();
  }

  public void close() throws IOException {
    closed = true;
    listener.close();
    for (var session : sessions) session.close();
  }

  private static final class Session {
    final Socket front, back;
    volatile int sender = -2;
    volatile boolean broker;

    Session(Socket front, Socket back) {
      this.front = front;
      this.back = back;
    }

    void close() {
      try {
        front.close();
      } catch (IOException ignored) {
      }
      try {
        back.close();
      } catch (IOException ignored) {
      }
    }
  }
}
