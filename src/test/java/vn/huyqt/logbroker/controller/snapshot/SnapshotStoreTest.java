package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.persistence.QuorumStateStore;
import vn.huyqt.logbroker.controller.support.*;

class SnapshotStoreTest {
    @TempDir Path root;
    @Test void publishedSnapshotReopensAndPinnedChunksMatchCompleteFile() throws Exception {
        var io=new FaultFiles();var identity=ControllerTestSupport.identity(0);
        QuorumStateStore.format(root,identity,io); SnapshotId id;
        var image=new MetadataImage(9,List.of(new TopicCreated(new UUID(0,9),"orders",3)));
        try(var state=QuorumStateStore.open(root,identity,io)) {
            var store=new SnapshotStore(root,identity,io,state,65536);
            id=store.create(image,3);assertEquals(image,store.load(id));
            try(var pin=store.pin(id)) {
                byte[] all=Files.readAllBytes(root.resolve("snapshots").resolve(id.contentId()+".snapshot"));
                assertEquals(all.length,pin.length());
                assertArrayEquals(Arrays.copyOfRange(all,0,32),pin.read(0,32));
                assertThrows(IllegalArgumentException.class,()->pin.read(-1,32));
            }
        }
        try(var state=QuorumStateStore.open(root,identity,io)) {
            var store=new SnapshotStore(root,identity,io,state,65536);
            assertEquals(List.of(id),store.retained()); assertEquals(image,store.load(id));
        }
    }
    @Test void publishedCorruptionIsFatalAndPartialFilesNeverBecomeSnapshots() throws Exception {
        var io=new FaultFiles();var identity=ControllerTestSupport.identity(0);QuorumStateStore.format(root,identity,io);
        try(var state=QuorumStateStore.open(root,identity,io)) {
            var store=new SnapshotStore(root,identity,io,state,65536);
            SnapshotId id=store.create(new MetadataImage(2,List.of()),1);
            Files.write(root.resolve("snapshots/orphan.partial"),new byte[]{1});
            byte[] bytes=Files.readAllBytes(root.resolve("snapshots").resolve(id.contentId()+".snapshot"));
            bytes[bytes.length-1]^=1;Files.write(root.resolve("snapshots").resolve(id.contentId()+".snapshot"),bytes);
            assertThrows(IOException.class,()->store.load(id));
            assertThrows(IOException.class,()->new SnapshotStore(root,identity,io,state,65536));
        }
    }
    @Test void imageBoundaryAndClusterIdentityMustMatchSnapshot() throws Exception {
        var io=new FaultFiles();var identity=ControllerTestSupport.identity(0);QuorumStateStore.format(root,identity,io);
        try(var state=QuorumStateStore.open(root,identity,io)) {
            var store=new SnapshotStore(root,identity,io,state,65536);
            var id=new SnapshotId(9,3,new UUID(0,9));
            assertThrows(IOException.class,()->store.encode(id,new MetadataImage(8,List.of())));
            byte[] wrong=store.encode(id,new MetadataImage(9,List.of()));
            java.nio.ByteBuffer.wrap(wrong).putLong(22,99); // cluster UUID low half
            assertThrows(IOException.class,()->store.decode(id,wrong));
        }
    }
}
