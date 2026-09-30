package vn.huyqt.logbroker.controller.consensus;

import java.util.*;

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
