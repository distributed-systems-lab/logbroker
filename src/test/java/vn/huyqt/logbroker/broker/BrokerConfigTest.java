package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

class BrokerConfigTest {
    @Test
    void labDefaultsRespectFrameAndFetchBudgets() {
        var config = BrokerConfig.defaults(Path.of("data"));
        assertEquals(9092, config.port());
        assertEquals(Duration.ofMillis(10), config.flushInterval());
        assertEquals(1024 * 1024, config.flushBytes());
        assertEquals(8 * 1024 * 1024, config.protocolLimits().maxFrameBytes());
        assertEquals(4 * 1024 * 1024, config.maxFetchBytes());
        assertEquals(64, config.protocolLimits().maxPartitionEntries());
        assertEquals(10_000, config.protocolLimits().maxRecordsPerBatch());
        assertEquals(4, config.dataWorkers());
        assertEquals(2, config.validationWorkers());
        assertEquals(128, config.maxConnections());
    }

    @Test
    void testOverridesStayImmutableAndValidatePort() {
        var original = BrokerConfig.defaults(Path.of("data"));
        var changed = original.withPort(0).withFlushInterval(Duration.ofMillis(1));
        assertEquals(9092, original.port());
        assertEquals(0, changed.port());
        assertEquals(Duration.ofMillis(1), changed.flushInterval());
        assertThrows(IllegalArgumentException.class, () -> original.withPort(-1));
    }
}
