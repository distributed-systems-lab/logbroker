package vn.huyqt.logbroker.broker.metadata;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.cluster.BrokerControlClient;
import vn.huyqt.logbroker.controller.client.ControllerClientException;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.*;

/** No local metadata writes: topic identity and assignments come from committed controller replies. */
public final class ClusterMetadataService implements BrokerMetadata {
    private final BrokerControlClient control;
    private final Supplier<MetadataImage> image;
    private final DeadlineScheduler clock;
    public ClusterMetadataService(BrokerControlClient control,Supplier<MetadataImage> image,DeadlineScheduler clock) {
        this.control=Objects.requireNonNull(control); this.image=Objects.requireNonNull(image); this.clock=Objects.requireNonNull(clock);
    }
    public MetadataImage image() { return image.get(); }
    public CompletableFuture<CreateResult> create(String name,int partitions,long deadlineNanos) {
        try {
            new TopicCatalog.TopicCreated(new UUID(0,1),name,partitions);
            if(clock.nanoTime()>=deadlineNanos) return CompletableFuture.failedFuture(new ControllerClientException(
                QuorumError.REQUEST_TIMED_OUT,ControllerClientException.Outcome.NOT_SENT,"Topic command deadline expired"));
            int timeout=(int)Math.max(1,Math.min(30_000,(deadlineNanos-clock.nanoTime())/1_000_000));
            return control.request(new BrokerControlProtocol.CreateTopic(name,partitions,(short)1,timeout),deadlineNanos)
                .thenApply(reply-> {
                    if(!(reply instanceof BrokerControlProtocol.CreateTopicReply created)) throw invalidReply();
                    try { return new CreateResult(created.topicId(),created.commitOffset()); }
                    catch(IllegalArgumentException invalid) { throw invalidReply(); }
                });
        } catch(IllegalArgumentException invalid) { return CompletableFuture.failedFuture(invalid); }
    }
    private ControllerClientException invalidReply() {
        return new ControllerClientException(QuorumError.INVALID_REQUEST,ControllerClientException.Outcome.UNKNOWN,"Invalid committed topic reply");
    }
}
