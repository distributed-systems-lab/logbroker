package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.log.*;

class BrokerLifecycleStateTest {
    @TempDir Path root;
    @Test void startupWithoutQuorumCannotGrantPermission() throws Exception {
        try(var h=new ObserverHarness(root)) {
            var gate=new ServingGate(h.session);
            var lifecycle=new BrokerLifecycle(new BrokerIdentityStore.Identity(new UUID(0,1),19,new UUID(0,2)),h.control,h.observer,h.clock,gate);
            var started=lifecycle.start(); h.clock.runDue();
            h.clock.advance(Duration.ofSeconds(60)); h.clock.runDue();
            assertFalse(gate.canServe()); assertFalse(started.isDone());
            assertEquals(BrokerLifecycle.State.FENCED,lifecycle.state());
            var stopped=lifecycle.stop(); h.clock.runDue(); while(!h.disk.isEmpty()) h.runOneDiskTask();
            assertTrue(stopped.isDone());
        }
    }
    @Test void committedCacheCannotRunBeforeFreshRegistrationAndRecoveryGrant() throws Exception {
        try(var h=new ObserverHarness(root)) {
            var gate=new ServingGate(h.session);
            var lifecycle=new BrokerLifecycle(new BrokerIdentityStore.Identity(new UUID(0,1),19,new UUID(0,2)),h.control,h.observer,h.clock,gate);
            lifecycle.metadataApplied(new MetadataImage(8,List.of(),(short)2,Map.of(19,new MetadataImage.BrokerRegistrationView(
                new BrokerRegistration(h.session,BrokerControlClientTest.config().advertised(),(short)2,(short)2),false,8)),Map.of()));
            assertFalse(gate.canServe()); assertEquals(BrokerLifecycle.State.STARTING,lifecycle.state());
            lifecycle.stop(); h.clock.runDue(); while(!h.disk.isEmpty()) h.runOneDiskTask();
        }
    }
    @Test void registrationUsesOneIncarnationAndOriginalCasAcrossRetries() throws Exception {
        try(var h=new ObserverHarness(root)) {
            var lifecycle=new BrokerLifecycle(new BrokerIdentityStore.Identity(new UUID(0,1),19,new UUID(0,2)),h.control,h.observer,h.clock,new ServingGate(h.session));
            lifecycle.start(); h.clock.runDue(); h.transport.last().replyNext(BrokerControlClientTest.describe()); h.clock.runDue();
            h.transport.last().replyNext(new BrokerControlProtocol.MetadataReply(BrokerControlClientTest.describe().meta(),Consistency.LINEARIZABLE,1,0,
                new MetadataImage(0,List.of(),(short)2,Map.of(),Map.of()))); h.clock.runDue();
            var first=assertInstanceOf(Register.class,h.transport.last().sent().getLast());
            h.transport.last().disconnect(); h.clock.runDue(); h.clock.advance(Duration.ofMillis(100)); h.clock.runDue();
            h.transport.last().replyNext(BrokerControlClientTest.describe()); h.clock.runDue();
            var retried=assertInstanceOf(Register.class,h.transport.last().sent().getLast());
            assertEquals(first.incarnationId(),retried.incarnationId()); assertEquals(first.expectedBrokerEpoch(),retried.expectedBrokerEpoch());
            lifecycle.stop(); h.clock.runDue(); while(!h.disk.isEmpty()) h.runOneDiskTask();
        }
    }
    @Test void freshGrantMustBeAppliedAndControllerLossDoesNotCreateServingLease() throws Exception {
        try(var h=new ObserverHarness(root)) {
            var gate=new ServingGate(h.session);
            var lifecycle=new BrokerLifecycle(new BrokerIdentityStore.Identity(new UUID(0,1),19,new UUID(0,2)),h.control,h.observer,h.clock,gate);
            var ready=lifecycle.start(); h.clock.runDue();
            h.transport.last().replyNext(BrokerControlClientTest.describe()); h.clock.runDue();
            h.transport.last().replyNext(metadata(new MetadataImage(0,List.of(),(short)2,Map.of(),Map.of()))); h.clock.runDue();
            var request=assertInstanceOf(Register.class,h.transport.last().sent().getLast());
            var session=new Session(19,new UUID(0,2),request.incarnationId(),2);
            var registration=new BrokerRegistration(session,BrokerControlClientTest.config().advertised(),(short)2,(short)2);
            h.transport.last().replyNext(new RegisterReply(BrokerControlClientTest.describe().meta(),session,2,2)); h.clock.runDue();
            var machine=new MetadataStateMachine(MetadataLimits.defaults(),(short)2);
            var prefix=new QuorumBatch(0,List.of(new QuorumEntry.FeatureLevel(1,new FeatureLevel((short)2)),new QuorumEntry.BrokerRegistration(1,registration)));
            machine.apply(prefix);
            h.transport.last().replyNext(metadata(machine.image())); h.clock.runDue();
            var first=assertInstanceOf(Heartbeat.class,h.transport.last().sent().getLast()); assertFalse(first.recoveryComplete());
            h.transport.last().replyNext(new HeartbeatReply(BrokerControlClientTest.describe().meta(),SessionStatus.FENCED,2,2,2,first.recoveryId())); h.clock.runDue();
            h.runOneDiskTask();
            h.transport.last().replyNext(new ObserverFetchReply(BrokerControlClientTest.describe().meta(),2,new FetchData(List.of(prefix))));
            h.clock.runDue(); h.runOneDiskTask();
            assertFalse(gate.canServe()); assertEquals(2,lifecycle.recoveryTarget());
            emptyFetch(h,2); h.clock.advance(Duration.ofSeconds(1)); h.clock.runDue(); emptyFetch(h,2);
            var recovery=assertInstanceOf(Heartbeat.class,h.transport.last().sent().getLast()); assertTrue(recovery.recoveryComplete());
            h.transport.last().replyNext(new HeartbeatReply(BrokerControlClientTest.describe().meta(),SessionStatus.ACTIVE,2,3,2,recovery.recoveryId())); h.clock.runDue();
            assertFalse(gate.canServe()); assertFalse(ready.isDone());
            h.clock.advance(Duration.ofMillis(100)); h.clock.runDue();
            var unfence=new QuorumBatch(2,List.of(new QuorumEntry.BrokerState(1,new BrokerState(19,2,false))));
            h.transport.last().replyNext(new ObserverFetchReply(BrokerControlClientTest.describe().meta(),3,new FetchData(List.of(unfence))));
            h.clock.runDue(); h.runOneDiskTask();
            assertTrue(gate.canServe()); assertEquals(BrokerLifecycle.State.RUNNING,lifecycle.state()); assertTrue(ready.isDone());
            h.transport.last().disconnect(); h.clock.runDue(); h.clock.advance(Duration.ofSeconds(60)); h.clock.runDue();
            assertTrue(gate.canServe()); assertEquals(BrokerLifecycle.State.RUNNING,lifecycle.state());
            machine.apply(unfence);
            machine.apply(new QuorumBatch(3,List.of(new QuorumEntry.BrokerState(1,new BrokerState(19,2,true)))));
            lifecycle.metadataApplied(machine.image());
            assertFalse(gate.canServe()); assertEquals(BrokerLifecycle.State.FENCED,lifecycle.state());
            assertEquals(4,lifecycle.recoveryTarget());
            machine.apply(new QuorumBatch(4,List.of(new QuorumEntry.ReadBarrier(1))));
            lifecycle.metadataApplied(machine.image()); assertEquals(4,lifecycle.recoveryTarget());
            machine.apply(new QuorumBatch(5,List.of(new QuorumEntry.BrokerState(1,new BrokerState(19,2,false)))));
            lifecycle.metadataApplied(machine.image());
            assertFalse(gate.canServe()); // Image alone cannot reuse the old recovery grant.
            var stopped=lifecycle.stop(); h.clock.runDue(); while(!h.disk.isEmpty()) h.runOneDiskTask();
            assertTrue(stopped.isDone()); assertFalse(gate.canServe());
        }
    }
    private static BrokerControlProtocol.MetadataReply metadata(MetadataImage image) {
        return new BrokerControlProtocol.MetadataReply(BrokerControlClientTest.describe().meta(),Consistency.LINEARIZABLE,1,image.appliedOffset(),image);
    }
    @Test void storageMismatchFailsClosedBeforeRegistration() throws Exception {
        try(var h=new ObserverHarness(root)) {
            var gate=new ServingGate(h.session);
            var lifecycle=new BrokerLifecycle(new BrokerIdentityStore.Identity(new UUID(0,1),19,new UUID(0,2)),h.control,h.observer,h.clock,gate);
            var started=lifecycle.start(); h.clock.runDue(); h.transport.last().replyNext(BrokerControlClientTest.describe()); h.clock.runDue();
            var foreign=new Session(19,new UUID(0,99),new UUID(0,3),2);
            var image=new MetadataImage(2,List.of(),(short)2,Map.of(19,new MetadataImage.BrokerRegistrationView(
                new BrokerRegistration(foreign,BrokerControlClientTest.config().advertised(),(short)2,(short)2),true,2)),Map.of());
            h.transport.last().replyNext(metadata(image)); h.clock.runDue();
            assertEquals(BrokerLifecycle.State.FAILED,lifecycle.state()); assertTrue(started.isCompletedExceptionally()); assertFalse(gate.canServe());
            assertTrue(h.transport.last().sent().stream().noneMatch(Register.class::isInstance));
        }
    }
    private static void emptyFetch(ObserverHarness h,long commit) {
        assertInstanceOf(ObserverFetch.class,h.transport.last().sent().getLast());
        h.transport.last().replyNext(new ObserverFetchReply(BrokerControlClientTest.describe().meta(),commit,new FetchData(List.of()))); h.clock.runDue();
    }
}
