package vn.huyqt.logbroker.integration.support;

import java.nio.file.*;
import java.util.Properties;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.broker.cluster.BrokerClusterConfig;

/** Process fixture publishes diagnostics from the actual production broker composition. */
public final class ClusterProcessMain {
    public static void main(String[] args) throws Exception {
        var properties = new Properties();
        try (var input = Files.newInputStream(Path.of(args[0]))) {
            properties.load(input);
        }
        var config = BrokerClusterConfig.fromProperties(properties);
        var root = Path.of(args[1]);
        var broker =
                Broker.start(
                        BrokerConfig.defaults(root).withPort(config.advertised().port()), config);
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close));
        var status = root.resolve("process-status.txt");
        String previous = "";
        while (true) {
            var snapshot = broker.status();
            String diagnostic =
                    "state="
                            + snapshot.lifecycle()
                            + " session="
                            + snapshot.session()
                            + " offset="
                            + snapshot.appliedOffset()
                            + " snapshotEnd="
                            + snapshot.snapshotEnd();
            if (!diagnostic.equals(previous)) {
                System.out.println(diagnostic);
                System.out.flush();
                previous = diagnostic;
            }
            var temporary = root.resolve("process-status.tmp");
            Files.writeString(
                    temporary,
                    broker.canServe()
                            + " "
                            + snapshot.appliedOffset()
                            + " "
                            + snapshot.observerGeneration()
                            + " "
                            + snapshot.snapshotEnd()
                            + " "
                            + snapshot.heartbeatAgeMillis());
            Files.move(
                    temporary,
                    status,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            Thread.sleep(100);
        }
    }
}
