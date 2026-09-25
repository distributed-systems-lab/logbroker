package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

class BrokerLifecycleTest {
    @TempDir Path directory;

    @Test void locksDataRootUntilShutdownAndCanRestart() throws Exception {
        var config = BrokerConfig.defaults(directory).withPort(0);
        try (var broker = Broker.start(config)) {
            assertTrue(broker.address().getPort() > 0);
            assertThrows(IOException.class, () -> Broker.start(config));
            broker.shutdown(Duration.ofSeconds(5)).get();
        }
        try (var restarted = Broker.start(config)) {
            assertTrue(restarted.address().getPort() > 0);
        }
    }
}
