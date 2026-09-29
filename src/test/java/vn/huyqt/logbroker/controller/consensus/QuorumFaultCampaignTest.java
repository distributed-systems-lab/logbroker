package vn.huyqt.logbroker.controller.consensus;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;import java.util.*;import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.support.*;
class QuorumFaultCampaignTest {
    @Test void seededSchedulesPreserveSafetyAcknowledgementsAndLinearizability(){var seeds=new ArrayList<>(List.of(1L,7L,42L,20260928L));int count=Integer.getInteger("quorum.seedCount",4);for(int i=4;i<count;i++)seeds.add((long)i*7919);for(long seed:seeds){var h=QuorumHarness.threeNodes(seed);h.runSchedule(seed,2000);h.assertSafety();h.heal();h.assertAcknowledgedTopicsSurvive();assertTrue(LinearizabilityHistoryTest.check(h.history()),()->"seed="+seed+" history="+h.history());}}
    @Test void independentOracleDetectsAcknowledgementBeforeDurability(){var h=QuorumHarness.threeNodes(1);h.elect(0);h.recordAcknowledgedForTest("ghost",UUID.randomUUID(),1);for(int i=0;i<3;i++){h.powerLoss(i);h.restart(i);}assertThrows(AssertionError.class,h::assertAcknowledgedTopicsSurvive);}
    @Test void independentOracleDetectsForgottenVoteAfterRestart(){var h=QuorumHarness.threeNodes(7);h.elect(0);h.assertSafety();var disk=h.disk(1);long epoch=disk.epoch();int vote=disk.votedFor();assertTrue(vote>=0);h.crash(1);disk.forgetVoteForTest();h.restart(1);disk.overwriteVoteForTest(epoch,(vote+1)%3);assertThrows(AssertionError.class,h::assertSafety);}
    @Test void minorityLongPartitionDoesNotLoseAcknowledgedTopic(){var h=QuorumHarness.threeNodes(42);h.elect(0);var created=h.trackedCreate(0,"stable",2);h.settle();for(int i=0;i<20&&!created.isDone();i++){h.tick(Duration.ofMillis(100));h.settle();}assertTrue(created.isDone());h.isolate(0);for(int i=0;i<100;i++){h.tick(Duration.ofMillis(100));h.settle();}h.heal();h.runHealthySuffix();h.assertAcknowledgedTopicsSurvive();}
}
