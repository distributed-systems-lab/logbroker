package vn.huyqt.logbroker.controller.snapshot;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.persistence.QuorumStateStore;
import vn.huyqt.logbroker.controller.support.*;

class SnapshotPublicationTest {
    @TempDir Path temporary;
    @Test void failureBeforeJournalPublicationNeverMakesSnapshotUsable() throws Exception {
        for(int failure=1;failure<=3;failure++) {
            Path root=temporary.resolve("n"+failure);var io=new FaultFiles();var identity=ControllerTestSupport.identity(0);
            QuorumStateStore.format(root,identity,io);
            try(var state=QuorumStateStore.open(root,identity,io)) {
                var store=new SnapshotStore(root,identity,io,state,65536);io.failAfter(failure);
                assertThrows(IOException.class,()->store.create(new MetadataImage(2,List.of()),1));
                assertTrue(store.retained().isEmpty());
            }
            io.clearFailure();
            try(var state=QuorumStateStore.open(root,identity,io)) {
                assertTrue(new SnapshotStore(root,identity,io,state,65536).retained().isEmpty());
            }
        }
    }
    @Test void keepsTwoPublishedSnapshotsAndPinsPreventPrematureDeletion() throws Exception {
        var root=temporary.resolve("n");var io=new FaultFiles();var identity=ControllerTestSupport.identity(0);
        QuorumStateStore.format(root,identity,io);
        try(var state=QuorumStateStore.open(root,identity,io)) {
            var store=new SnapshotStore(root,identity,io,state,65536);
            var first=store.create(new MetadataImage(1,List.of()),1);
            try(var pin=store.pin(first)) {
                var second=store.create(new MetadataImage(2,List.of()),1);
                var third=store.create(new MetadataImage(3,List.of()),2);
                assertEquals(List.of(third,second),store.retained());
                assertTrue(pin.read(0,16).length>0);
            }
        }
    }
}
