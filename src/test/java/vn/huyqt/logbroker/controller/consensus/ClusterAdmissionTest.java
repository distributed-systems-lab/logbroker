package vn.huyqt.logbroker.controller.consensus;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.support.QuorumHarness;
class ClusterAdmissionTest {
    @Test void timedOutRegistrationMayStillCommitAndRetryKeepsItsEpoch() {
        var h = QuorumHarness.threeNodes(7, (short)2);
        h.tickNode(0, Duration.ofSeconds(4)); h.settle();
        var request = new vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.Register(1,
            new java.util.UUID(0,1), new java.util.UUID(0,2), -1,
            new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint("localhost",9092), (short)2,(short)2,2000);
        h.pauseDisk(1); h.pauseDisk(2);
        var result = h.service(0).request(request,h.now()+1_000_000); h.settle();
        h.tickNode(0,Duration.ofMillis(2)); h.settle();
        var failure = assertThrows(java.util.concurrent.CompletionException.class,result::join);
        assertInstanceOf(vn.huyqt.logbroker.controller.ControllerService.ServiceException.class,failure.getCause());
        assertTrue(failure.getCause().getMessage().startsWith("REQUEST_TIMED_OUT"));
        h.resumeDisk(1); h.resumeDisk(2); h.settle();
        assertEquals(3,h.image(0).brokers().get(1).registration().session().brokerEpoch());
        h.isolate(0); h.tickNode(1,Duration.ofSeconds(4)); h.settle();
        var retry = h.service(1).request(request,h.now()+Duration.ofSeconds(2).toNanos()); h.settle();
        var reply = (vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.RegisterReply)retry.join();
        assertEquals(3,reply.session().brokerEpoch());
        assertEquals(1,h.disk(1).batches().stream().flatMap(b -> b.entries().stream())
            .filter(vn.huyqt.logbroker.controller.log.QuorumEntry.BrokerRegistration.class::isInstance).count());
    }
    @Test void readinessRequiresCommittedFeatureAndFailoverDoesNotRebootstrap() {
        var h = QuorumHarness.threeNodes(7, (short) 2);
        h.tickNode(0, Duration.ofSeconds(4)); h.settle();
        assertTrue(h.node(0).status().ready());
        assertEquals((short) 2, h.image(0).metadataVersion());
        assertEquals(2, h.node(0).status().applied());
        h.isolate(0); h.tickNode(1, Duration.ofSeconds(4)); h.settle();
        assertTrue(h.node(1).status().ready());
        assertEquals(1, h.disk(1).batches().stream().flatMap(b -> b.entries().stream())
            .filter(vn.huyqt.logbroker.controller.log.QuorumEntry.FeatureLevel.class::isInstance).count());
    }
    @Test void registrationReplyWaitsForMajorityAndAppliedImage() throws Exception {
        var h = QuorumHarness.threeNodes(7, (short) 2);
        h.tickNode(0, Duration.ofSeconds(4)); h.settle();
        var request = new vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.Register(1,
            new java.util.UUID(0,1), new java.util.UUID(0,2), -1,
            new vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint("localhost", 9092), (short) 2, (short) 2, 2000);
        h.pauseDisk(1); h.pauseDisk(2);
        var result = h.service(0).request(request, h.now() + Duration.ofSeconds(2).toNanos());
        h.settle(); assertFalse(result.isDone());
        h.resumeDisk(1);
        assertFalse(result.isDone()); h.settle();
        var reply = (vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.RegisterReply) result.join();
        assertEquals(3, reply.session().brokerEpoch());
        assertEquals(reply.session(), h.image(0).brokers().get(1).registration().session());
    }
}
