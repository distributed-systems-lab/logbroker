package vn.huyqt.logbroker.broker.cluster;

import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Session;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.storage.LogConfig;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.nio.file.*;
import java.util.*;

final class ObserverHarness implements AutoCloseable {
    final ManualScheduler clock = new ManualScheduler();
    final ScriptedControlTransport.Factory transport = new ScriptedControlTransport.Factory();
    final ArrayDeque<Runnable> disk = new ArrayDeque<>();
    final FaultFiles files = new FaultFiles();
    final List<MetadataImage> published = new ArrayList<>();
    final List<Throwable> fatal = new ArrayList<>();
    final ObserverStore store;
    final BrokerControlClient control;
    final MetadataObserver observer;
    final Session session = new Session(19, new UUID(0, 2), new UUID(0, 3), 1);

    ObserverHarness(Path root) throws Exception {
        ObserverStore.format(root, new UUID(0, 1), files, LogConfig.defaults());
        store =
                ObserverStore.open(
                        root,
                        new UUID(0, 1),
                        files,
                        LogConfig.defaults(),
                        MetadataLimits.defaults());
        control =
                new BrokerControlClient(
                        BrokerControlClientTest.config(), transport, clock, () -> 0.0);
        observer =
                new MetadataObserver(control, store, clock, disk::add, published::add, fatal::add);
    }

    void start() {
        observer.start(session);
        clock.runDue();
        transport.last().replyNext(BrokerControlClientTest.describe());
        clock.runDue();
        transport
                .last()
                .replyNext(
                        new BrokerControlProtocol.MetadataReply(
                                BrokerControlClientTest.describe().meta(),
                                Consistency.LINEARIZABLE,
                                1,
                                1,
                                new MetadataImage(1, List.of(), (short) 2, Map.of(), Map.of())));
        clock.runDue();
        runOneDiskTask();
        clock.runDue();
    }

    void completeFetchWithOneCommittedBatch(long commit) {
        transport
                .last()
                .replyNext(
                        new BrokerControlProtocol.ObserverFetchReply(
                                BrokerControlClientTest.describe().meta(),
                                commit,
                                new FetchData(
                                        List.of(
                                                new QuorumBatch(
                                                        0,
                                                        List.of(
                                                                new QuorumEntry.FeatureLevel(
                                                                        1,
                                                                        new ClusterRecords
                                                                                .FeatureLevel(
                                                                                (short) 2))))))));
        clock.runDue();
    }

    long fetches() {
        return transport.connections.stream()
                .flatMap(w -> w.sent().stream())
                .filter(BrokerControlProtocol.ObserverFetch.class::isInstance)
                .count();
    }

    void runOneDiskTask() {
        disk.removeFirst().run();
        clock.runDue();
    }

    public void close() throws Exception {
        var stopped = observer.stop();
        clock.runDue();
        while (!disk.isEmpty()) runOneDiskTask();
        if (!stopped.isDone()) throw new AssertionError("Observer stop did not drain");
        control.close();
        clock.runDue();
        store.close();
        clock.close();
    }
}
