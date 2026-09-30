package vn.huyqt.logbroker.controller.support;

import java.util.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEffect;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

public final class SimulatedTransport {
  public record Envelope(int source, int target, Frame frame, ReplyRoute route) {}

  private final ArrayDeque<Envelope> pending = new ArrayDeque<>();
  private final List<Frame> sent = new ArrayList<>();
  private final Set<Integer> isolated = new HashSet<>();

  public void send(int source, QuorumEffect.Send effect) {
    Frame frame = effect.frame();
    sent.add(frame);
    if (isolated.contains(source) || isolated.contains(effect.peerId())) return;
    pending.addLast(
        new Envelope(source, effect.peerId(), frame, new ReplyRoute(source, frame.requestId())));
  }

  public void reply(int source, QuorumEffect.Reply effect) {
    int target = (int) effect.route().connectionId();
    sent.add(effect.frame());
    if (isolated.contains(source) || isolated.contains(target)) return;
    pending.addLast(new Envelope(source, target, effect.frame(), effect.route()));
  }

  public Envelope next() {
    return pending.pollFirst();
  }

  public void dropNext() {
    pending.pollFirst();
  }

  public void reorder() {
    if (pending.size() > 1) pending.addLast(pending.removeFirst());
  }

  public int pending() {
    return pending.size();
  }

  public void isolate(int node) {
    isolated.add(node);
    pending.removeIf(e -> e.source() == node || e.target() == node);
  }

  public void heal() {
    isolated.clear();
  }

  public List<Frame> voteRequests() {
    return sent.stream().filter(f -> !f.response() && f.operation() == 101).toList();
  }

  public List<Frame> history() {
    return List.copyOf(sent);
  }

  public void inject(Envelope envelope) {
    pending.addLast(envelope);
  }
}
