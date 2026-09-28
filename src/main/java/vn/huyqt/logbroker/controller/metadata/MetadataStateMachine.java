package vn.huyqt.logbroker.controller.metadata;

import java.io.IOException;
import java.util.ArrayList;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.log.*;

/** Consensus supplies only committed batches; applying a bad batch publishes no partial image. */
public final class MetadataStateMachine {
    private MetadataImage image = new MetadataImage(0, java.util.List.of());
    private TopicCatalog catalog = new TopicCatalog();
    public void apply(QuorumBatch batch) throws IOException {
        if (batch.baseOffset() != image.appliedOffset()) throw new IOException("Apply gap");
        var next = new TopicCatalog(); for (var topic : image.topics()) next.apply(topic);
        for (var entry : batch.entries()) if (entry instanceof QuorumEntry.Topic topic) next.apply(topic.event());
        var topics = new ArrayList<TopicCreated>();
        for (var topic : next.snapshot()) topics.add(next.find(topic.name()));
        try { image = new MetadataImage(batch.nextOffset(), topics); }
        catch (IllegalArgumentException e) { throw new IOException("Metadata capacity exceeded", e); }
        catalog = next;
    }
    public void restore(MetadataImage restored) throws IOException {
        var next = new TopicCatalog(); for (var topic : restored.topics()) next.apply(topic);
        image = restored; catalog = next;
    }
    public MetadataImage image() { return image; }
    public TopicCreated find(String name) { return catalog.find(name); }
}
