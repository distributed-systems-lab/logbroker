package vn.huyqt.logbroker.controller.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import vn.huyqt.logbroker.broker.metadata.MetadataEventCodec;

public final class QuorumEntryCodec {
    private QuorumEntryCodec() {}
    public static byte[] encode(QuorumEntry entry) {
        byte[] payload; byte kind;
        switch (entry) {
            case QuorumEntry.LeaderChange leader -> { kind = 1; payload = ByteBuffer.allocate(4).putInt(leader.leaderId()).array(); }
            case QuorumEntry.Topic topic -> { kind = 2; payload = MetadataEventCodec.encode(topic.event()); }
            case QuorumEntry.ReadBarrier ignored -> { kind = 3; payload = new byte[0]; }
        }
        return ByteBuffer.allocate(11 + payload.length).putShort((short) 1).put(kind).putLong(entry.epoch()).put(payload).array();
    }
    public static QuorumEntry decode(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < 11 || bytes.length > 1024 * 1024) throw new IOException("Invalid entry length");
        try {
            var in = ByteBuffer.wrap(bytes);
            if (in.getShort() != 1) throw new IOException("Unsupported entry version");
            byte kind = in.get(); long epoch = in.getLong();
            QuorumEntry result = switch (kind) {
                case 1 -> new QuorumEntry.LeaderChange(epoch, in.getInt());
                case 2 -> { byte[] payload = new byte[in.remaining()]; in.get(payload);
                    yield new QuorumEntry.Topic(epoch, MetadataEventCodec.decode(payload)); }
                case 3 -> new QuorumEntry.ReadBarrier(epoch);
                default -> throw new IOException("Unknown entry kind");
            };
            if (in.hasRemaining()) throw new IOException("Trailing entry bytes");
            return result;
        } catch (BufferUnderflowException | IllegalArgumentException e) { throw new IOException("Invalid entry", e); }
    }
}
