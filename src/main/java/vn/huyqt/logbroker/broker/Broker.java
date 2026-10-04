package vn.huyqt.logbroker.broker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.broker.cluster.BrokerClusterConfig;
import vn.huyqt.logbroker.broker.cluster.ClusterBrokerRuntime;
import vn.huyqt.logbroker.broker.metadata.BrokerMetadata;
import vn.huyqt.logbroker.controller.client.ControllerClientTransport;
import vn.huyqt.logbroker.controller.persistence.DurableFiles;

/** Owns the formatted broker root, observer, listener and assigned partition resources. */
public final class Broker implements AutoCloseable {
    private final BrokerConfig config;
    private final ClusterBrokerRuntime runtime;

    private Broker(BrokerConfig config, ClusterBrokerRuntime runtime) {
        this.config = config;
        this.runtime = runtime;
    }

    /** Starts a formatted observer broker. Listener binding does not grant serving permission. */
    public static Broker start(BrokerConfig config, BrokerClusterConfig cluster) throws IOException {
        return start(config, cluster, new DurableFiles(), null);
    }

    /** Explicit durability and transport injection for deterministic fault fixtures. */
    public static Broker start(BrokerConfig config, BrokerClusterConfig cluster,
            DurableFiles files, ControllerClientTransport.Factory factory) throws IOException {
        return new Broker(config, ClusterBrokerRuntime.start(config, cluster, files, factory));
    }

    public InetSocketAddress address() { return runtime.address(); }
    public boolean canServe() { return runtime.canServe(); }
    public BrokerMetadata clusterMetadata() { return runtime.metadata(); }
    public vn.huyqt.logbroker.broker.cluster.BrokerStatus status() { return runtime.status(); }

    /** Drains observer and partition work before closing storage and releasing the root lock. */
    public CompletableFuture<Void> shutdown(Duration deadline) {
        if (deadline == null || deadline.isZero() || deadline.isNegative())
            throw new IllegalArgumentException("Positive shutdown deadline required");
        return runtime.shutdown(deadline);
    }

    @Override public void close() {
        try {
            shutdown(config.shutdownTimeout()).get();
        } catch (Exception error) {
            throw new IllegalStateException("Broker shutdown failed", error);
        }
    }
}
