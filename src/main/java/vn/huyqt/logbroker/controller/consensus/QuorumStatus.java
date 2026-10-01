package vn.huyqt.logbroker.controller.consensus;

import java.util.*;

/**
 * Immutable view of one node's quorum state, as published by {@link QuorumStateMachine} and
 * returned by DescribeQuorum.
 *
 * <p>All offsets are exclusive batch boundaries. {@code durableMatches} holds the remote durable
 * matches last recorded while this node was leader; it is not a synchronized snapshot of the
 * whole cluster. {@code leaderId} is -1 when no leader is known. {@code failure} carries the cause
 * once the role is {@link Role#FAILED}.
 */
public record QuorumStatus(
    int nodeId,
    Role role,
    long epoch,
    int leaderId,
    UUID generation,
    long logEnd,
    long durableEnd,
    long commit,
    long applied,
    long snapshotEnd,
    boolean ready,
    Map<Integer, Long> durableMatches,
    String failure) {
  /** Consensus role. A node never leaves {@code FAILED} or {@code STOPPING} for an active role. */
  public enum Role {
    UNATTACHED,
    FOLLOWER,
    CANDIDATE,
    LEADER,
    FAILED,
    STOPPING
  }

  public QuorumStatus {
    Objects.requireNonNull(role);
    Objects.requireNonNull(generation);
    durableMatches = Map.copyOf(durableMatches);
    Objects.requireNonNull(failure);
  }
}
