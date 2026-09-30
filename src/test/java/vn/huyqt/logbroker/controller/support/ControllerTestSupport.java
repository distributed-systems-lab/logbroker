package vn.huyqt.logbroker.controller.support;

import java.util.*;
import vn.huyqt.logbroker.controller.ClusterIdentity;

public final class ControllerTestSupport {
  private ControllerTestSupport() {}

  public static ClusterIdentity identity(int node) {
    return new ClusterIdentity(
        new UUID(0, 1),
        node,
        List.of(
            new ClusterIdentity.Voter(0, "localhost", 19090),
            new ClusterIdentity.Voter(1, "localhost", 19091),
            new ClusterIdentity.Voter(2, "localhost", 19092)));
  }
}
