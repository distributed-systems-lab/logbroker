package vn.huyqt.logbroker.controller.metadata;

import java.io.*;
import java.util.ArrayList;
import vn.huyqt.logbroker.broker.metadata.MetadataEventCodec;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;

public final class MetadataImageCodec {
  private MetadataImageCodec() {}

  public static byte[] encode(MetadataImage image) {
    try {
      var bytes = new ByteArrayOutputStream();
      var out = new DataOutputStream(bytes);
      out.writeLong(image.appliedOffset());
      out.writeInt(image.topics().size());
      for (var topic : image.topics()) {
        byte[] entry = MetadataEventCodec.encode(topic);
        out.writeInt(entry.length);
        out.write(entry);
      }
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new UncheckedIOException(impossible);
    }
  }

  public static MetadataImage decode(byte[] bytes) throws IOException {
    if (bytes == null || bytes.length < 12 || bytes.length > 64 * 1024 * 1024)
      throw new IOException("Invalid image size");
    try (var in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      long offset = in.readLong();
      int count = in.readInt();
      if (count < 0 || count > 128 || count > in.available() / 31)
        throw new IOException("Invalid topic count");
      var topics = new ArrayList<TopicCreated>(count);
      for (int i = 0; i < count; i++) {
        int n = in.readInt();
        if (n < 27 || n > 275 || n > in.available()) throw new IOException("Invalid event length");
        topics.add(MetadataEventCodec.decode(in.readNBytes(n)));
      }
      if (in.available() != 0) throw new IOException("Trailing image bytes");
      return new MetadataImage(offset, topics);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid image", e);
    }
  }
}
