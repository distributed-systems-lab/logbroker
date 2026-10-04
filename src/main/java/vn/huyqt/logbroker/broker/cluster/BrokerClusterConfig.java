package vn.huyqt.logbroker.broker.cluster;

import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint;
import vn.huyqt.logbroker.controller.metadata.MetadataLimits;

import java.time.Duration;
import java.util.*;

/** Immutable control-plane settings. Heartbeat timings never constitute a data-serving lease. */
public record BrokerClusterConfig(
        UUID clusterId,
        int brokerId,
        Endpoint advertised,
        List<Endpoint> controllers,
        Duration heartbeatInterval,
        Duration heartbeatRpcTimeout,
        Duration sessionTimeout,
        MetadataLimits limits) {
    public static final Set<String> PROPERTY_KEYS =
            Set.of(
                    "cluster.id",
                    "broker.id",
                    "advertised.host",
                    "advertised.port",
                    "controller.bootstrap.servers",
                    "broker.heartbeat.interval.ms",
                    "broker.heartbeat.rpc.timeout.ms",
                    "broker.session.timeout.ms");

    public BrokerClusterConfig {
        Objects.requireNonNull(clusterId);
        Objects.requireNonNull(advertised);
        Objects.requireNonNull(limits);
        controllers = List.copyOf(controllers);
        if (clusterId.equals(new UUID(0, 0))
                || brokerId < 0
                || controllers.isEmpty()
                || controllers.size() > 3
                || controllers.stream().distinct().count() != controllers.size()
                || heartbeatInterval.isNegative()
                || heartbeatInterval.isZero()
                || heartbeatInterval.compareTo(heartbeatRpcTimeout) >= 0
                || heartbeatRpcTimeout.compareTo(sessionTimeout) >= 0)
            throw new IllegalArgumentException("Invalid broker cluster settings");
        heartbeatInterval.toNanos();
        heartbeatRpcTimeout.toNanos();
        sessionTimeout.toNanos();
    }

    public static BrokerClusterConfig fromProperties(Properties properties) {
        var endpoints = new ArrayList<Endpoint>();
        for (String token : required(properties, "controller.bootstrap.servers").split(",", -1)) {
            token = token.strip();
            int colon = token.lastIndexOf(':');
            if (colon <= 0) throw new IllegalArgumentException("Controllers must be host:port");
            String host = token.substring(0, colon);
            if (host.startsWith("[") && host.endsWith("]"))
                host = host.substring(1, host.length() - 1);
            endpoints.add(new Endpoint(host, Integer.parseInt(token.substring(colon + 1))));
        }
        return new BrokerClusterConfig(
                UUID.fromString(required(properties, "cluster.id")),
                Integer.parseInt(required(properties, "broker.id")),
                new Endpoint(
                        required(properties, "advertised.host"),
                        Integer.parseInt(required(properties, "advertised.port"))),
                endpoints,
                millis(properties, "broker.heartbeat.interval.ms", 1000),
                millis(properties, "broker.heartbeat.rpc.timeout.ms", 2000),
                millis(properties, "broker.session.timeout.ms", 10000),
                new MetadataLimits(
                        32,
                        Integer.parseInt(properties.getProperty("maxTopics", "128")),
                        Integer.parseInt(properties.getProperty("maxPartitions", "1024")),
                        64 * 1024 * 1024));
    }

    private static String required(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
        return value;
    }

    private static Duration millis(Properties p, String key, long fallback) {
        return Duration.ofMillis(Long.parseLong(p.getProperty(key, Long.toString(fallback))));
    }
}
