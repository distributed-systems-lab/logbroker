package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.support.ManualScheduler;

class PartitionProvisioningCrashTest {
    @TempDir Path root;
    @Test void eachProvisioningForceBoundaryRecoversWithoutInventingCompletion() throws Exception {
        var measured=new FaultFiles(); Path measuredRoot=root.resolve("measured"); initialize(measuredRoot,measured);
        int boundaries;
        try(var clock=new ManualScheduler(); var lanes=new PartitionExecutor(1,8,8)) {
            var inventory=PartitionInventory.open(measuredRoot,ClusterPartitionManagerTest.STORAGE,measured);
            var disk=new ArrayDeque<Runnable>(); var manager=manager(measuredRoot,measured,inventory,disk,clock,lanes);
            int before=measured.trace().size(); manager.reconcile(ClusterPartitionManagerTest.image(),ClusterPartitionManagerTest.session());
            disk.removeFirst().run(); boundaries=measured.trace().size()-before;
            assertTrue(manager.runtime(ClusterPartitionManagerTest.TP).isPresent()); assertTrue(boundaries>=6);
            var closed=manager.closeAsync(); disk.removeFirst().run(); closed.get(2,TimeUnit.SECONDS);
        }
        for(int boundary=1;boundary<=boundaries;boundary++) {
            Path caseRoot=root.resolve("force-"+boundary); var files=new FaultFiles(); initialize(caseRoot,files);
            try(var clock=new ManualScheduler(); var lanes=new PartitionExecutor(1,8,8)) {
                var inventory=PartitionInventory.open(caseRoot,ClusterPartitionManagerTest.STORAGE,files);
                var disk=new ArrayDeque<Runnable>(); var manager=manager(caseRoot,files,inventory,disk,clock,lanes);
                files.failAfter(boundary); manager.reconcile(ClusterPartitionManagerTest.image(),ClusterPartitionManagerTest.session()); disk.removeFirst().run();
                assertTrue(manager.runtime(ClusterPartitionManagerTest.TP).isEmpty(),"boundary "+boundary);
                assertNotEquals(PartitionInventory.State.COMPLETE,inventory.state(ClusterPartitionManagerTest.TP));
                files.clearFailure(); var closed=manager.closeAsync(); disk.removeFirst().run(); closed.get(2,TimeUnit.SECONDS);
            }
            files.powerLoss();
            try(var clock=new ManualScheduler(); var lanes=new PartitionExecutor(1,8,8)) {
                var inventory=PartitionInventory.open(caseRoot,ClusterPartitionManagerTest.STORAGE,files);
                assertNotEquals(PartitionInventory.State.COMPLETE,inventory.state(ClusterPartitionManagerTest.TP));
                var disk=new ArrayDeque<Runnable>(); var manager=manager(caseRoot,files,inventory,disk,clock,lanes);
                manager.reconcile(ClusterPartitionManagerTest.image(),ClusterPartitionManagerTest.session()); disk.removeFirst().run();
                assertTrue(manager.runtime(ClusterPartitionManagerTest.TP).isPresent(),"boundary "+boundary+" "+manager.failures());
                assertEquals(PartitionInventory.State.COMPLETE,inventory.state(ClusterPartitionManagerTest.TP));
                var closed=manager.closeAsync(); disk.removeFirst().run(); closed.get(2,TimeUnit.SECONDS);
            }
        }
    }
    private static void initialize(Path root,FaultFiles files) throws Exception {
        PartitionInventory.format(root,ClusterPartitionManagerTest.STORAGE,files); Files.createDirectory(root.resolve("partitions")); files.syncDirectory(root);
    }
    private static ClusterPartitionManager manager(Path root,FaultFiles files,PartitionInventory inventory,ArrayDeque<Runnable> disk,
        ManualScheduler clock,PartitionExecutor lanes) {
        return new ClusterPartitionManager(inventory,FilePartitionStore.clusterFactory(),disk::add,root,new ServingGate(ClusterPartitionManagerTest.session()),
            files,BrokerConfig.defaults(root),lanes,clock);
    }
}
