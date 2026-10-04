package vn.huyqt.logbroker.controller.protocol;

import vn.huyqt.logbroker.controller.ClusterIdentity.Voter;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.*;
import vn.huyqt.logbroker.controller.metadata.MetadataImage;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.snapshot.SnapshotId;

import java.util.*;

/** Schema-v2 broker control messages. Observer traffic never establishes voter contact. */
public final class BrokerControlProtocol {
    private BrokerControlProtocol() {}

    public enum SenderRole {
        ADMIN,
        VOTER,
        BROKER
    }

    public enum SessionStatus {
        ACTIVE,
        FENCED,
        STALE_SESSION,
        REGISTRATION_REQUIRED
    }

    public record Register(
            int brokerId,
            UUID storageId,
            UUID incarnationId,
            long expectedBrokerEpoch,
            Endpoint endpoint,
            short minVersion,
            short maxVersion,
            int timeoutMs)
            implements Request {}

    public record RegisterReply(
            ReplyMeta meta, Session session, long registrationOffset, long requiredMetadataOffset)
            implements Reply {}

    public record Heartbeat(
            Session session,
            long sequence,
            long appliedOffset,
            UUID recoveryId,
            boolean recoveryComplete,
            int timeoutMs)
            implements Request {}

    public record HeartbeatReply(
            ReplyMeta meta,
            SessionStatus status,
            long brokerEpoch,
            long stateOffset,
            long requiredMetadataOffset,
            UUID recoveryId)
            implements Reply {}

    public record ObserverFetch(
            Session session,
            long controllerEpoch,
            long nextOffset,
            long prefixEpoch,
            int maxBytes,
            int maxWaitMs)
            implements Request {}

    public record ObserverFetchReply(ReplyMeta meta, long commitOffset, FetchPayload payload)
            implements Reply {
        public ObserverFetchReply {
            if (payload instanceof Divergence)
                throw new IllegalArgumentException("Observer history cannot truncate");
        }
    }

    public record ObserverSnapshot(
            Session session, long controllerEpoch, SnapshotId id, long position, int maxBytes)
            implements Request {}

    public record ObserverSnapshotReply(
            ReplyMeta meta, SnapshotId id, long position, long totalLength, byte[] chunk)
            implements Reply {
        public ObserverSnapshotReply {
            chunk = chunk.clone();
        }

        public byte[] chunk() {
            return chunk.clone();
        }

        public int chunkLength() {
            return chunk.length;
        }
    }

    public record CreateTopic(String name, int partitions, short replicationFactor, int timeoutMs)
            implements Request {}

    public record CreateTopicReply(ReplyMeta meta, UUID topicId, long commitOffset)
            implements Reply {}

    public record DescribeReply(
            ReplyMeta meta, QuorumStatus status, List<Voter> voters, byte[] voterHash)
            implements Reply {
        public DescribeReply {
            voters = voters.stream().sorted(Comparator.comparingInt(Voter::id)).toList();
            if (voters.size() != 3 || voterHash.length != 32)
                throw new IllegalArgumentException("Invalid membership");
            voterHash = voterHash.clone();
        }

        public byte[] voterHash() {
            return voterHash.clone();
        }
    }

    public record MetadataReply(
            ReplyMeta meta,
            Consistency consistency,
            int nodeId,
            long commitOffset,
            MetadataImage image)
            implements Reply {}

    /** Request whitelist; only controllers emit replies, including replies to broker operations. */
    public static boolean allowed(Frame frame) {
        return allowed(frame.version(), frame.senderRole(), frame.operation(), frame.response());
    }

    public static boolean allowed(short version, SenderRole role, short op, boolean response) {
        if (version != 2 || op < 101 || op > 113) return false;
        if (response) return role == SenderRole.VOTER;
        return switch (role) {
            case ADMIN -> op >= 106 && op <= 109;
            case VOTER -> op <= 109;
            case BROKER -> op >= 106;
        };
    }
}
