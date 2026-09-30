package vn.huyqt.logbroker.controller.protocol;

import java.util.*;
import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.log.QuorumBatch;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

public final class QuorumProtocol {
  private QuorumProtocol() {}

  public record ReplyRoute(long connectionId, long requestId) {}

  public sealed interface Message permits Request, Reply {}

  public sealed interface Request extends Message
      permits Vote,
          BeginQuorumEpoch,
          EndQuorumEpoch,
          QuorumFetch,
          FetchSnapshot,
          DescribeQuorum,
          CreateTopic,
          ReadMetadata,
          ReadLocalMetadata {}

  public sealed interface Reply extends Message
      permits VoteReply,
          EpochReply,
          QuorumFetchReply,
          FetchSnapshotReply,
          DescribeQuorumReply,
          CreateTopicReply,
          MetadataReply,
          Failure {
    ReplyMeta meta();
  }

  public record Frame(
      short operation,
      boolean response,
      UUID clusterId,
      int senderId,
      long requestId,
      byte[] voterHash,
      Message message) {
    public Frame {
      Objects.requireNonNull(clusterId);
      Objects.requireNonNull(message);
      if (voterHash.length != 32 || requestId < 0 || senderId < -1)
        throw new IllegalArgumentException("Invalid frame identity");
      voterHash = voterHash.clone();
    }

    @Override
    public byte[] voterHash() {
      return voterHash.clone();
    }
  }

  public record ReplyMeta(QuorumError error, String message, long epoch, int leaderId) {
    public ReplyMeta {
      Objects.requireNonNull(error);
      Objects.requireNonNull(message);
    }
  }

  public record Vote(long epoch, long lastEpoch, long end) implements Request {}

  public record BeginQuorumEpoch(long epoch) implements Request {}

  public record EndQuorumEpoch(long epoch) implements Request {}

  public record QuorumFetch(
      long epoch, long end, long lastEpoch, int maxBytes, int maxWaitMs, long challenge)
      implements Request {}

  public record FetchSnapshot(long epoch, SnapshotId id, long position, int maxBytes)
      implements Request {}

  public record DescribeQuorum() implements Request {}

  public record CreateTopic(String name, int partitions, int timeoutMs) implements Request {}

  public record ReadMetadata(int timeoutMs) implements Request {}

  public record ReadLocalMetadata() implements Request {}

  public record VoteReply(ReplyMeta meta, boolean granted) implements Reply {}

  public record EpochReply(ReplyMeta meta) implements Reply {}

  public sealed interface FetchPayload permits FetchData, Divergence, SnapshotRequired {}

  public record FetchData(List<QuorumBatch> batches) implements FetchPayload {
    public FetchData {
      batches = List.copyOf(batches);
    }
  }

  public record Divergence(long commonEpoch, long commonEnd) implements FetchPayload {}

  public record SnapshotRequired(SnapshotId id) implements FetchPayload {}

  public record QuorumFetchReply(ReplyMeta meta, long challenge, long commit, FetchPayload payload)
      implements Reply {}

  public record FetchSnapshotReply(
      ReplyMeta meta, SnapshotId id, long position, long totalLength, byte[] chunk)
      implements Reply {
    public FetchSnapshotReply {
      chunk = chunk.clone();
    }

    public int chunkLength() {
      return chunk.length;
    }

    @Override
    public byte[] chunk() {
      return chunk.clone();
    }
  }

  public record DescribeQuorumReply(ReplyMeta meta, QuorumStatus status) implements Reply {}

  public record CreateTopicReply(ReplyMeta meta, UUID topicId) implements Reply {}

  public enum Consistency {
    LOCAL,
    LINEARIZABLE
  }

  public record MetadataView(
      Consistency consistency,
      int nodeId,
      long epoch,
      int leaderId,
      long commit,
      long applied,
      List<TopicCreated> topics) {
    public MetadataView {
      topics = List.copyOf(topics);
    }
  }

  public record MetadataReply(ReplyMeta meta, MetadataView view) implements Reply {}

  public record Failure(ReplyMeta meta) implements Reply {}

  public static short operation(Request request) {
    return switch (request) {
      case Vote ignored -> 101;
      case BeginQuorumEpoch ignored -> 102;
      case EndQuorumEpoch ignored -> 103;
      case QuorumFetch ignored -> 104;
      case FetchSnapshot ignored -> 105;
      case DescribeQuorum ignored -> 106;
      case CreateTopic ignored -> 107;
      case ReadMetadata ignored -> 108;
      case ReadLocalMetadata ignored -> 109;
    };
  }
}
