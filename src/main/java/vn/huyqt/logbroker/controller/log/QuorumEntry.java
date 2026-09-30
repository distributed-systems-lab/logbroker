package vn.huyqt.logbroker.controller.log;

import java.util.Objects;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

public sealed interface QuorumEntry {
  long epoch();

  record LeaderChange(long epoch, int leaderId) implements QuorumEntry {
    public LeaderChange {
      if (epoch < 0 || leaderId < 0) throw new IllegalArgumentException("Invalid leader change");
    }
  }

  record Topic(long epoch, TopicCreated event) implements QuorumEntry {
    public Topic {
      if (epoch < 0) throw new IllegalArgumentException("Negative epoch");
      Objects.requireNonNull(event);
    }
  }

  record ReadBarrier(long epoch) implements QuorumEntry {
    public ReadBarrier {
      if (epoch < 0) throw new IllegalArgumentException("Negative epoch");
    }
  }
}
