package vn.huyqt.logbroker.broker.cluster;

import org.junit.jupiter.api.Test;
import java.util.*;

class ClusterFaultCampaignTest {
    @Test void independentOracleRejectsPartialCommittedTopicBatch() throws Exception {
        try (var harness = new ClusterFaultHarness(7)) {
            harness.injectPartialTopicForTest();
            org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class, harness::assertInvariants);
        }
    }
    @Test void independentOraclesDetectBrokenRetryApplyAndUnfence() throws Exception {
        try (var harness = new ClusterFaultHarness(7)) {
            harness.injectDuplicateRetryForTest();
            org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class, harness::assertInvariants);
        }
        try (var harness = new ClusterFaultHarness(7)) {
            harness.injectUncommittedApplyForTest();
            org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class, harness::assertInvariants);
        }
        try (var harness = new ClusterFaultHarness(7)) {
            harness.injectStaleUnfenceForTest();
            org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class, harness::assertInvariants);
        }
    }
    @Test void realQuorumAndObserversPreserveSafetyAcrossSeededSchedules() throws Exception {
        ArrayList<Long> seeds;
        try (var input = getClass().getResourceAsStream("/cluster/fault-seeds.txt")) {
            java.util.Objects.requireNonNull(input, "Checked-in cluster fault seeds");
            seeds = new ArrayList<>(new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).lines()
                    .filter(line -> !line.startsWith("#") && !line.isBlank()).map(Long::parseLong).toList());
        }
        int count = Integer.getInteger("cluster.seedCount", 4);
        for (int index = 4; index < count; index++) seeds.add(index * 7919L);
        for (long seed : seeds) try (var harness = new ClusterFaultHarness(seed)) {
            harness.run(2000); harness.healAndDrain(); harness.assertInvariants();
        }
    }
}
