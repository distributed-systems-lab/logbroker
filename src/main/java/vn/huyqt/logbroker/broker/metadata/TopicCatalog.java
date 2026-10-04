package vn.huyqt.logbroker.broker.metadata;

import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.protocol.Protocol.PartitionInfo;
import vn.huyqt.logbroker.protocol.Protocol.TopicInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Applies ordered durable metadata events independently of their source.
 *
 * <p>The catalog is kept separate from the local append and flush so that a later phase can feed it
 * committed quorum metadata instead; see section 4 of {@code
 * docs/superpowers/specs/2026-09-25-broker-phase-2-design.md}. All methods are synchronized.
 */
public final class TopicCatalog {
    /**
     * A topic creation event.
     *
     * <p>The constructor throws {@link IllegalArgumentException} unless the name is 1 to 249 ASCII
     * letters, digits, {@code .}, {@code _} or {@code -} and is neither {@code .} nor {@code ..},
     * the UUID is non-zero, and the partition count is positive.
     */
    public record TopicCreated(UUID id, String name, int partitions) {
        public TopicCreated {
            Objects.requireNonNull(id);
            Objects.requireNonNull(name);
            if (id.equals(new UUID(0, 0))
                    || !name.matches("[A-Za-z0-9._-]{1,249}")
                    || name.equals(".")
                    || name.equals("..")
                    || partitions <= 0) throw new IllegalArgumentException("Invalid topic event");
        }
    }

    private final Map<String, TopicCreated> byName = new HashMap<>();
    private final Map<UUID, TopicCreated> byId = new HashMap<>();

    /**
     * Applies {@code event}. Re-applying an identical event is a no-op.
     *
     * @throws IOException if another topic already uses the event's name or ID
     */
    public synchronized void apply(TopicCreated event) throws IOException {
        TopicCreated nameMatch = byName.get(event.name());
        TopicCreated idMatch = byId.get(event.id());
        if (event.equals(nameMatch) && event.equals(idMatch)) return;
        if (nameMatch != null || idMatch != null)
            throw new IOException("Conflicting metadata event: " + event.name());
        byName.put(event.name(), event);
        byId.put(event.id(), event);
    }

    public synchronized TopicCreated find(String name) {
        return byName.get(name);
    }

    public synchronized int topicCount() {
        return byName.size();
    }

    public synchronized int partitionCount() {
        return byName.values().stream().mapToInt(TopicCreated::partitions).sum();
    }

    /**
     * Returns an immutable copy of all topics sorted by name. Every partition is reported with
     * {@code NONE}; availability comes from the partition registry, not from the catalog.
     */
    public synchronized List<TopicInfo> snapshot() {
        List<TopicInfo> topics = new ArrayList<>();
        for (TopicCreated event : byName.values()) {
            List<PartitionInfo> partitions = new ArrayList<>();
            for (int i = 0; i < event.partitions(); i++)
                partitions.add(new PartitionInfo(i, Error.none()));
            topics.add(new TopicInfo(event.name(), event.id(), partitions));
        }
        topics.sort(java.util.Comparator.comparing(TopicInfo::name));
        return List.copyOf(topics);
    }
}
