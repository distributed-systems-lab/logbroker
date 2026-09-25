package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerMainTest {
    @TempDir Path directory;

    @Test void configFileAndCliOverridesApplyInOrder() throws Exception {
        var properties = directory.resolve("broker.properties");
        Files.writeString(properties, "port=19092\nflushIntervalMs=25\nmaxConnections=4\n");
        var config = BrokerMain.parse(new String[]{"--data", directory.resolve("data").toString(),
                "--config", properties.toString(), "--port", "0"});
        assertEquals(0, config.port());
        assertEquals(25, config.flushInterval().toMillis());
        assertEquals(4, config.maxConnections());
    }

    @Test void rejectsUnknownProperty() throws Exception {
        var properties = directory.resolve("broker.properties");
        Files.writeString(properties, "unboundedQueue=true\n");
        assertThrows(IllegalArgumentException.class,
                () -> BrokerMain.parse(new String[]{"--data", directory.toString(),
                        "--config", properties.toString()}));
    }
}
