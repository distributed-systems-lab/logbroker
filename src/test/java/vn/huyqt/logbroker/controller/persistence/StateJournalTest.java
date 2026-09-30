package vn.huyqt.logbroker.controller.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.support.FaultFiles;

class StateJournalTest {
  @TempDir Path root;

  @Test
  void forcedFramesSurviveReopenAndPartialFinalFrameIsDiscarded() throws Exception {
    Path file = root.resolve("state");
    var io = new FaultFiles();
    try (var journal = StateJournal.open(file, io)) {
      assertEquals(1, journal.append((short) 1, new byte[] {1, 2, 3}));
    }
    long valid = Files.size(file);
    Files.write(file, new byte[] {0x51, 0x4a, 0x4e}, StandardOpenOption.APPEND);
    try (var journal = StateJournal.open(file, io)) {
      assertEquals(1, journal.frames().size());
      assertEquals(valid, Files.size(file));
      assertArrayEquals(new byte[] {1, 2, 3}, journal.frames().getFirst().payload());
    }
  }

  @Test
  void completeChecksumFailureIsNotRepaired() throws Exception {
    Path file = root.resolve("state");
    var io = new FaultFiles();
    try (var journal = StateJournal.open(file, io)) {
      journal.append((short) 1, new byte[] {7});
    }
    byte[] bytes = Files.readAllBytes(file);
    bytes[20] ^= 1;
    Files.write(file, bytes);
    assertThrows(IOException.class, () -> StateJournal.open(file, io));
    assertArrayEquals(bytes, Files.readAllBytes(file));
  }

  @Test
  void rejectsInvalidPartialHeaderInsteadOfDroppingIt() throws Exception {
    Path file = root.resolve("state");
    Files.write(file, new byte[] {0x00, 0x00});
    assertThrows(IOException.class, () -> StateJournal.open(file, new FaultFiles()));
  }

  @Test
  void failedForceDoesNotPublishFrameAndPoisonsWriter() throws Exception {
    var io = new FaultFiles();
    try (var journal = StateJournal.open(root.resolve("state"), io)) {
      io.failAfter(1);
      assertThrows(IOException.class, () -> journal.append((short) 1, new byte[] {8}));
      assertTrue(journal.frames().isEmpty());
      assertThrows(IOException.class, () -> journal.append((short) 1, new byte[] {9}));
    }
  }
}
