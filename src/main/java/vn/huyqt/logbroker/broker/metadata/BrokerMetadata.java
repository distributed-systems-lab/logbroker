package vn.huyqt.logbroker.broker.metadata;

import vn.huyqt.logbroker.controller.metadata.MetadataImage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Local committed observer view and commands forwarded to its quorum authority. */
public interface BrokerMetadata {
    record CreateResult(UUID topicId, long committedOffset) {
        public CreateResult {
            if (topicId == null || topicId.equals(new UUID(0, 0)) || committedOffset <= 0)
                throw new IllegalArgumentException("Invalid committed topic result");
        }
    }

    MetadataImage image();

    CompletableFuture<CreateResult> create(String name, int partitions, long deadlineNanos);
}
