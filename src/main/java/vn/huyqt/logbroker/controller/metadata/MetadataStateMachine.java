package vn.huyqt.logbroker.controller.metadata;

import java.io.IOException;
import java.util.ArrayList;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.log.*;

/**
 * Consensus supplies only committed batches; applying a bad batch publishes no partial image.
 *
 * <p>Holds the applied {@link MetadataImage} and a matching {@link TopicCatalog}. Batches must be
 * applied in log order without gaps; control entries advance the applied offset without changing
 * topics. Not thread-safe; the caller serializes access.
 */
public final class MetadataStateMachine {
  private MetadataImage image = new MetadataImage(0, java.util.List.of());
  private TopicCatalog catalog = new TopicCatalog();

  /**
   * Applies one committed batch and advances the image to its end.
   *
   * @throws IOException if the batch does not start at the applied offset, contains a conflicting
   *     topic, or would exceed the image limits; the previous image stays published
   */
  public void apply(QuorumBatch batch) throws IOException {
    if (batch.baseOffset() != image.appliedOffset()) throw new IOException("Apply gap");
    // Build into a fresh catalog so a failure part-way through leaves the current state intact.
    var next = new TopicCatalog();
    for (var topic : image.topics()) next.apply(topic);
    for (var entry : batch.entries())
      if (entry instanceof QuorumEntry.Topic topic) next.apply(topic.event());
    var topics = new ArrayList<TopicCreated>();
    for (var topic : next.snapshot()) topics.add(next.find(topic.name()));
    try {
      image = new MetadataImage(batch.nextOffset(), topics);
    } catch (IllegalArgumentException e) {
      throw new IOException("Metadata capacity exceeded", e);
    }
    catalog = next;
  }

  /** Replaces all state with {@code restored}, such as a snapshot image. */
  public void restore(MetadataImage restored) throws IOException {
    var next = new TopicCatalog();
    for (var topic : restored.topics()) next.apply(topic);
    image = restored;
    catalog = next;
  }

  public MetadataImage image() {
    return image;
  }

  /** Returns the applied topic named {@code name}, or {@code null} if there is none. */
  public TopicCreated find(String name) {
    return catalog.find(name);
  }
}
