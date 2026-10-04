package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.metadata.ClusterMetadataService;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.support.ManualScheduler;

class ClusterMetadataServiceTest {
    @Test
    void controllerOwnsTopicIdentityAndCommittedOffset() {
        var clock = new ManualScheduler();
        var factory = new ScriptedControlTransport.Factory();
        try (var control =
                new BrokerControlClient(BrokerControlClientTest.config(), factory, clock)) {
            var image = MetadataImage.empty((short) 2);
            var service = new ClusterMetadataService(control, () -> image, clock);
            var result = service.create("orders", 3, 30_000_000_000L);
            clock.runDue();
            factory.last().replyNext(BrokerControlClientTest.describe());
            clock.runDue();
            var request =
                    assertInstanceOf(
                            BrokerControlProtocol.CreateTopic.class,
                            factory.last().sent().getLast());
            assertEquals(1, request.replicationFactor());
            assertEquals(3, request.partitions());
            UUID id = new UUID(0, 17);
            factory.last()
                    .replyNext(
                            new BrokerControlProtocol.CreateTopicReply(
                                    BrokerControlClientTest.describe().meta(), id, 20));
            clock.runDue();
            assertEquals(id, result.join().topicId());
            assertEquals(20, result.join().committedOffset());
            assertSame(image, service.image());
        }
    }

    @Test
    void invalidOrExpiredCommandNeverOpensControlConnection() {
        var clock = new ManualScheduler();
        var factory = new ScriptedControlTransport.Factory();
        try (var control =
                new BrokerControlClient(BrokerControlClientTest.config(), factory, clock)) {
            var service =
                    new ClusterMetadataService(
                            control, () -> MetadataImage.empty((short) 2), clock);
            assertTrue(service.create("orders", 3, 0).isCompletedExceptionally());
            assertTrue(service.create("../bad", 3, 30_000_000_000L).isCompletedExceptionally());
            assertTrue(service.create("orders", 0, 30_000_000_000L).isCompletedExceptionally());
            assertTrue(factory.connections.isEmpty());
        }
    }
}
