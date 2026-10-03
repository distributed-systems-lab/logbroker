package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.storage.LogConfig;

class ObserverInstallCrashTest {
    @TempDir Path root;
    @Test void cancellationAfterPreparationKeepsOldLogUsableAndCleanupPreservesPins() throws Exception {
        var files=new FaultFiles(); var cluster=new UUID(0,1); ObserverStore.format(root,cluster,files,LogConfig.defaults());
        try (var store=ObserverStore.open(root,cluster,files,LogConfig.defaults(),MetadataLimits.defaults())) {
            byte[] hash=new byte[32]; hash[0]=1; store.pinMembership(hash);
            var image=new MetadataImage(6,List.of(),(short)2,Map.of(),Map.of());
            var checks=new java.util.concurrent.atomic.AtomicInteger(); var id=store.snapshots().create(image,1);
            assertThrows(ObserverStore.InstallCancelled.class,() -> store.install(id,image,() -> checks.incrementAndGet()==1));
            store.appendCommitted(List.of(new vn.huyqt.logbroker.controller.log.QuorumBatch(0,List.of(
                new vn.huyqt.logbroker.controller.log.QuorumEntry.FeatureLevel(1,new ClusterRecords.FeatureLevel((short)2))))),1);
            assertEquals(1,store.image().appliedOffset());
            try (var pin=store.snapshots().pin(id)) {
                var nextImage=new MetadataImage(8,List.of(),(short)2,Map.of(),Map.of());
                var next=store.snapshots().create(nextImage,1); store.install(next,nextImage,() -> true);
                assertTrue(store.releaseObsoleteGenerations()); assertTrue(Files.isRegularFile(store.snapshots().path(id)));
                assertTrue(pin.length()>0);
                try (var generations=Files.list(root.resolve("generations"))) { assertEquals(1,generations.count()); }
            }
            assertTrue(store.releaseObsoleteGenerations()); assertFalse(Files.exists(store.snapshots().path(id)));
        }
    }
    @Test void corruptPublishedSnapshotIsFatalOnRestart() throws Exception {
        var files=new FaultFiles(); var cluster=new UUID(0,1); ObserverStore.format(root,cluster,files,LogConfig.defaults());
        Path path;
        try (var store=ObserverStore.open(root,cluster,files,LogConfig.defaults(),MetadataLimits.defaults())) {
            byte[] hash=new byte[32]; hash[0]=1; store.pinMembership(hash);
            var image=new MetadataImage(8,List.of(),(short)2,Map.of(),Map.of()); var id=store.snapshots().create(image,1);
            store.install(id,image,() -> true); path=store.snapshots().path(id);
        }
        byte[] corrupt=Files.readAllBytes(path); corrupt[corrupt.length-1]^=1; Files.write(path,corrupt);
        assertThrows(IOException.class,() -> ObserverStore.open(root,cluster,files,LogConfig.defaults(),MetadataLimits.defaults()));
    }
    @Test void eachForcedPublicationBoundaryReopensOneCompleteGeneration() throws Exception {
        int boundaries;
        var image=new MetadataImage(9,List.of(),(short)2,Map.of(),Map.of());
        var cluster=new UUID(0,1); byte[] hash=new byte[32]; hash[0]=1;
        var probe=new FaultFiles(); var probeRoot=root.resolve("probe"); ObserverStore.format(probeRoot,cluster,probe,LogConfig.defaults());
        try (var store=ObserverStore.open(probeRoot,cluster,probe,LogConfig.defaults(),MetadataLimits.defaults())) {
            store.pinMembership(hash); var id=store.snapshots().create(image,2);
            int before=probe.trace().size(); store.install(id,image,() -> true); boundaries=probe.trace().size()-before;
            assertTrue(boundaries>=5);
        }
        for (int failure=1;failure<=boundaries;failure++) {
            var directory=root.resolve("failure-"+failure); var files=new FaultFiles();
            ObserverStore.format(directory,cluster,files,LogConfig.defaults());
            try (var store=ObserverStore.open(directory,cluster,files,LogConfig.defaults(),MetadataLimits.defaults())) {
                store.pinMembership(hash); var id=store.snapshots().create(image,2); files.failAfter(failure);
                assertThrows(IOException.class,() -> store.install(id,image,() -> true)); files.clearFailure();
            }
            files.powerLoss();
            try (var store=ObserverStore.open(directory,cluster,files,LogConfig.defaults(),MetadataLimits.defaults())) {
                assertTrue(store.durableEnd()==0 || store.durableEnd()==9); assertEquals(store.durableEnd(),store.image().appliedOffset());
                if (store.durableEnd()==9) assertEquals(image,store.image());
            }
        }
    }
    @Test void cancelledInstallCannotPublishNewGeneration() throws Exception {
        var files=new FaultFiles(); var cluster=new UUID(0,1); ObserverStore.format(root,cluster,files,LogConfig.defaults());
        try (var store=ObserverStore.open(root,cluster,files,LogConfig.defaults(),MetadataLimits.defaults())) {
            byte[] hash=new byte[32]; hash[0]=1; store.pinMembership(hash); var old=store.generation();
            var image=new MetadataImage(8,List.of(),(short)2,Map.of(),Map.of());
            assertThrows(IOException.class,() -> store.install(new SnapshotId(8,2,new UUID(0,9)),image,() -> false));
            assertEquals(old,store.generation()); assertEquals(0,store.image().appliedOffset());
        }
    }
}
