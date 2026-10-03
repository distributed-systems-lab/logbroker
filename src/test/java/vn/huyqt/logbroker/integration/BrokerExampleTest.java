package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.LegacyBrokerFixture;
import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.example.ClientExample;

class BrokerExampleTest {
    @TempDir Path directory;

    @Test void exampleCreatesProducesAndFetchesThenCanRepeat() throws Exception {
        try (var broker = LegacyBrokerFixture.start(BrokerConfig.defaults(directory).withPort(0))) {
            int port = broker.address().getPort();
            assertEquals(1, ClientExample.run("127.0.0.1", port, "demo"));
            assertEquals(1, ClientExample.run("127.0.0.1", port, "demo"));
        }
    }
}
