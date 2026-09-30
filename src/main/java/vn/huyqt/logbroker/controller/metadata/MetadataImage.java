package vn.huyqt.logbroker.controller.metadata;

import java.util.*;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

public record MetadataImage(long appliedOffset, List<TopicCreated> topics) {
  public MetadataImage {
    if (appliedOffset < 0) throw new IllegalArgumentException("Negative applied offset");
    topics = topics.stream().sorted(Comparator.comparing(TopicCreated::name)).toList();
    if (topics.size() > 128
        || topics.stream().map(TopicCreated::name).distinct().count() != topics.size()
        || topics.stream().map(TopicCreated::id).distinct().count() != topics.size()
        || topics.stream().mapToLong(TopicCreated::partitions).sum() > 1024)
      throw new IllegalArgumentException("Invalid metadata image");
  }
}
