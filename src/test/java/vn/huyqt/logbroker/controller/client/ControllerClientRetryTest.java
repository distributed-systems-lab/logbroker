package vn.huyqt.logbroker.controller.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.support.ManualScheduler;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

class ControllerClientRetryTest {
    @Test
    void lostResponseThenRedirectCannotEraseUnknown() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client =
                new ControllerClient(
                        ControllerClientTest.ID, ControllerClientTest.BOOT, clock, factory)) {
            var future = client.createTopic("orders", 3);
            factory.last().acceptThenLoseResponse();
            clock.advance(Duration.ofSeconds(1));
            clock.runDue();
            factory.last().replyNotLeader();
            clock.advance(Duration.ofSeconds(31));
            clock.runDue();
            var error =
                    assertInstanceOf(
                            ControllerClientException.class,
                            assertThrows(
                                            ExecutionException.class,
                                            () -> future.get(2, TimeUnit.SECONDS))
                                    .getCause());
            assertEquals(ControllerClientException.Outcome.UNKNOWN, error.outcome());
            assertEquals(0, clock.pending());
        }
    }

    @Test
    void connectionFailureBeforeEnqueueIsNotSent() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        factory.refuse = true;
        try (var client =
                new ControllerClient(
                        ControllerClientTest.ID, ControllerClientTest.BOOT, clock, factory)) {
            var future = client.createTopic("orders", 3);
            clock.advance(Duration.ofSeconds(31));
            clock.runDue();
            var error =
                    assertInstanceOf(
                            ControllerClientException.class,
                            assertThrows(
                                            ExecutionException.class,
                                            () -> future.get(2, TimeUnit.SECONDS))
                                    .getCause());
            assertEquals(ControllerClientException.Outcome.NOT_SENT, error.outcome());
        }
    }

    @Test
    void retriesCreateSameNameCountAndBarrierMetadata() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client =
                new ControllerClient(
                        ControllerClientTest.ID, ControllerClientTest.BOOT, clock, factory)) {
            var create = client.createTopic("orders", 3);
            factory.last().reply(new Failure(new ReplyMeta(QuorumError.OVERLOADED, "", 1, -1)));
            clock.advance(Duration.ofSeconds(1));
            clock.runDue();
            var request = assertInstanceOf(CreateTopic.class, factory.last().sent.message());
            assertEquals("orders", request.name());
            assertEquals(3, request.partitions());
            UUID id = UUID.randomUUID();
            factory.last().reply(new CreateTopicReply(FakeClientTransport.ok(), id));
            assertEquals(id, create.get(2, TimeUnit.SECONDS));
            var read = client.metadata();
            factory.last().replyNotLeader();
            clock.advance(Duration.ofSeconds(1));
            clock.runDue();
            assertInstanceOf(ReadMetadata.class, factory.last().sent.message());
            factory.last()
                    .reply(
                            new MetadataReply(
                                    FakeClientTransport.ok(),
                                    new MetadataView(
                                            Consistency.LINEARIZABLE, 1, 2, 1, 5, 5, List.of())));
            assertEquals(Consistency.LINEARIZABLE, read.get(2, TimeUnit.SECONDS).consistency());
        }
    }

    @Test
    void localRequestNeverFollowsLeaderRedirect() throws Exception {
        var clock = new ManualScheduler();
        var factory = new FakeClientTransport.Factory();
        try (var client =
                new ControllerClient(
                        ControllerClientTest.ID, ControllerClientTest.BOOT, clock, factory)) {
            var read = client.localMetadata(2);
            factory.last().replyNotLeader();
            assertThrows(ExecutionException.class, () -> read.get(2, TimeUnit.SECONDS));
            assertEquals(1, factory.transports.size());
        }
    }
}
