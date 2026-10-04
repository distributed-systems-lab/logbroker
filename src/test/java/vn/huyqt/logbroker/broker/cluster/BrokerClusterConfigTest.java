package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class BrokerClusterConfigTest {
    private static Properties properties() {
        var p = new Properties();
        p.setProperty("cluster.id", new UUID(0, 1).toString());
        p.setProperty("broker.id", "7");
        p.setProperty("advertised.host", "localhost");
        p.setProperty("advertised.port", "9092");
        p.setProperty(
                "controller.bootstrap.servers", "localhost:19090,localhost:19091,localhost:19092");
        return p;
    }

    @Test
    void defaultsRespectHeartbeatOrderingAndClusterCapacity() {
        var config = BrokerClusterConfig.fromProperties(properties());
        assertEquals(7, config.brokerId());
        assertEquals(3, config.controllers().size());
        assertEquals(1000, config.heartbeatInterval().toMillis());
        assertEquals(2000, config.heartbeatRpcTimeout().toMillis());
        assertEquals(10000, config.sessionTimeout().toMillis());
        assertEquals(32, config.limits().maxBrokers());
    }

    @Test
    void malformedEndpointsAndInvalidTimeoutOrderingFail() {
        for (var change :
                Map.of(
                                "advertised.port",
                                "0",
                                "advertised.host",
                                "x".repeat(256),
                                "broker.heartbeat.interval.ms",
                                "2000",
                                "broker.session.timeout.ms",
                                "2000",
                                "controller.bootstrap.servers",
                                "localhost:0")
                        .entrySet()) {
            var p = properties();
            p.setProperty(change.getKey(), change.getValue());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> BrokerClusterConfig.fromProperties(p),
                    change.getKey());
        }
        var malformed = properties();
        malformed.setProperty("advertised.host", "\ud800");
        assertThrows(
                IllegalArgumentException.class,
                () -> BrokerClusterConfig.fromProperties(malformed));
    }
}
