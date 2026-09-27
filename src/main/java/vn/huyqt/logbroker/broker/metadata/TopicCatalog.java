package vn.huyqt.logbroker.broker.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import vn.huyqt.logbroker.protocol.Protocol.Error;
import vn.huyqt.logbroker.protocol.Protocol.PartitionInfo;
import vn.huyqt.logbroker.protocol.Protocol.TopicInfo;

/** Applies ordered durable metadata events independently of their source. */
public final class TopicCatalog {
    public record TopicCreated(UUID id, String name, int partitions) {
        public TopicCreated {
            Objects.requireNonNull(id);
            Objects.requireNonNull(name);
            if (id.equals(new UUID(0, 0)) || !name.matches("[A-Za-z0-9._-]{1,249}")
                    || name.equals(".") || name.equals("..") || partitions <= 0)
                throw new IllegalArgumentException("Invalid topic event");
        }
    }

    private final Map<String, TopicCreated> byName = new HashMap<>();
    private final Map<UUID, TopicCreated> byId = new HashMap<>();

    public synchronized void apply(TopicCreated event) throws IOException {
        TopicCreated nameMatch = byName.get(event.name());
        TopicCreated idMatch = byId.get(event.id());
        if (event.equals(nameMatch) && event.equals(idMatch))
            return;
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
