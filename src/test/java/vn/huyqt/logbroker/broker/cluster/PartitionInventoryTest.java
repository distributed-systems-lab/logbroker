package vn.huyqt.logbroker.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.support.FaultFiles;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

class PartitionInventoryTest {
    @TempDir Path root;
    private final UUID storage=new UUID(0,9);
    private final TopicPartition partition=new TopicPartition(new UUID(0,7),0);
    @Test void completionSurvivesReopen() throws Exception {
        var files=new FaultFiles(); PartitionInventory.format(root,storage,files);
        try(var inventory=PartitionInventory.open(root,storage,files)) {
            assertEquals(PartitionInventory.State.NEW,inventory.state(partition));
            assertThrows(IllegalStateException.class,()->inventory.complete(partition));
            inventory.begin(partition); inventory.begin(partition); inventory.complete(partition); inventory.complete(partition);
        }
        assertEquals(39+43+43,Files.size(root.resolve("partition-inventory.journal")));
        try(var inventory=PartitionInventory.open(root,storage,files)) { assertEquals(PartitionInventory.State.COMPLETE,inventory.state(partition)); }
    }
    @Test void missingInventoryAndStorageMismatchFailWithoutCreatingFiles() throws Exception {
        var files=new FaultFiles(); assertThrows(IOException.class,()->PartitionInventory.open(root,storage,files));
        assertFalse(Files.exists(root.resolve("partition-inventory.journal")));
        PartitionInventory.format(root,storage,files);
        assertThrows(IOException.class,()->PartitionInventory.open(root,new UUID(0,10),files));
    }
    @Test void forceFailurePoisonsWriterAndDoesNotPublishComplete() throws Exception {
        var files=new FaultFiles(); PartitionInventory.format(root,storage,files);
        try(var inventory=PartitionInventory.open(root,storage,files)) {
            inventory.begin(partition); files.failAfter(1);
            assertThrows(IOException.class,()->inventory.complete(partition));
            assertEquals(PartitionInventory.State.INTENT,inventory.state(partition)); files.clearFailure();
            assertThrows(IOException.class,()->inventory.complete(partition));
        }
        files.powerLoss();
        try(var inventory=PartitionInventory.open(root,storage,files)) { assertEquals(PartitionInventory.State.INTENT,inventory.state(partition)); }
    }
    @Test void validIncompleteTailIsDiscardedButCompleteCorruptionIsFatal() throws Exception {
        var files=new FaultFiles(); PartitionInventory.format(root,storage,files);
        try(var inventory=PartitionInventory.open(root,storage,files)) { inventory.begin(partition); inventory.complete(partition); }
        Path path=root.resolve("partition-inventory.journal"); byte[] bytes=Files.readAllBytes(path);
        Files.write(path,Arrays.copyOf(bytes,bytes.length-4));
        try(var inventory=PartitionInventory.open(root,storage,files)) { assertEquals(PartitionInventory.State.INTENT,inventory.state(partition)); }
        assertEquals(82,Files.size(path)); bytes[bytes.length-1]^=1; Files.write(path,bytes);
        assertThrows(IOException.class,()->PartitionInventory.open(root,storage,files));
    }
    @Test void malformedPartialHeaderIsNotRepaired() throws Exception {
        var files=new FaultFiles(); PartitionInventory.format(root,storage,files);
        Files.write(root.resolve("partition-inventory.journal"),new byte[]{1,2,3},StandardOpenOption.APPEND);
        assertThrows(IOException.class,()->PartitionInventory.open(root,storage,files));
    }
    @Test void partialSequenceMustStillMatchExpectedHeader() throws Exception {
        var files=new FaultFiles(); PartitionInventory.format(root,storage,files);
        try(var inventory=PartitionInventory.open(root,storage,files)) { inventory.begin(partition); }
        Path path=root.resolve("partition-inventory.journal"); byte[] frame=Arrays.copyOfRange(Files.readAllBytes(path),39,82);
        frame[10]=1; Files.write(path,Arrays.copyOf(frame,17),StandardOpenOption.APPEND);
        assertThrows(IOException.class,()->PartitionInventory.open(root,storage,files));
    }
}
