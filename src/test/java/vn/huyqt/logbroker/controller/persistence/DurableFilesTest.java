package vn.huyqt.logbroker.controller.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableFilesTest {
  @TempDir Path root;

  @Test
  void exclusiveWriteDoesNotOverwriteExistingFile() throws Exception {
    var io = new DurableFiles();
    Path file = root.resolve("identity");
    io.writeNew(file, new byte[] {1});
    assertThrows(IOException.class, () -> io.writeNew(file, new byte[] {2}));
    assertArrayEquals(new byte[] {1}, Files.readAllBytes(file));
  }

  @Test
  void strictDirectoryPublicationIsSupportedOrFailsExplicitly() throws Exception {
    var io = new DurableFiles();
    try {
      io.verifySupport(root);
      System.out.println("DIRECTORY_SYNC_SUPPORTED " + root.getFileSystem().provider().getScheme());
    } catch (IOException error) {
      assertTrue(error.getMessage().contains("Directory durability unsupported"));
      System.out.println("DIRECTORY_SYNC_UNSUPPORTED " + error.getMessage());
      assertThrows(IOException.class, () -> io.syncDirectory(root));
    }
  }
}
