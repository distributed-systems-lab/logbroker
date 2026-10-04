package vn.huyqt.logbroker.controller.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.*;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol;

/**
 * Fixed envelope staging selects the reserved control pool before allocating a frame body.
 *
 * <p>Reassembles length-prefixed controller frames and emits each as an {@link OwnedFrame} holding
 * the raw bytes and their budget lease. The length prefix and fixed envelope, 69 bytes together,
 * are staged in a fixed buffer and validated (length, version, direction, cluster ID, voter set
 * hash, operation and sender) before the frame buffer is allocated. A small peer control frame is
 * charged to the control budget, falling back to the shared budget; every other frame uses the
 * shared budget. Invalid frames and budget exhaustion close the connection, as does a frame not
 * completed within 30 seconds of its first byte, or no first byte within 30 seconds of activation.
 * Frame layout and limits are in {@code docs/controller-protocol-v1.md}.
 *
 * <p>One instance per channel; Netty calls it only from that channel's event loop.
 */
public final class QuorumFrameDecoder extends ChannelInboundHandlerAdapter {
    private final ControllerConfig config;
    private final ResourceBudget control, shared;
    private final DeadlineScheduler clock;
    private final byte[] header = new byte[70];
    private int headerCount, count, total;
    private byte[] bytes;
    private ResourceBudget.Lease lease;
    private DeadlineScheduler.Ticket deadline;
    private boolean peerControl;

    /**
     * @param control budget reserved for small peer control frames
     * @param shared budget for all other frames, and for control frames when {@code control} is
     *     full
     * @param clock deadline source for partial frames; null disables the deadline
     */
    public QuorumFrameDecoder(
            ControllerConfig config,
            ResourceBudget control,
            ResourceBudget shared,
            DeadlineScheduler clock) {
        this.config = config;
        this.control = control;
        this.shared = shared;
        this.clock = clock;
    }

    @Override
    public void channelActive(ChannelHandlerContext context) throws Exception {
        if (clock != null)
            deadline = clock.schedule(clock.nanoTime() + 30_000_000_000L, context::close);
        super.channelActive(context);
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (!(message instanceof ByteBuf input)) {
            ReferenceCountUtil.release(message);
            context.close();
            return;
        }
        try {
            while (input.isReadable() && context.channel().isOpen()) {
                if (bytes == null) {
                    if (headerCount == 0 && clock != null) {
                        if (deadline != null) deadline.cancel();
                        deadline =
                                clock.schedule(clock.nanoTime() + 30_000_000_000L, context::close);
                    }
                    int wanted = headerCount < 4 ? 4 : headerCount < 8 ? 8 : headerBytes();
                    int copy = Math.min(wanted - headerCount, input.readableBytes());
                    input.readBytes(header, headerCount, copy);
                    headerCount += copy;
                    if (headerCount < 4) return;
                    if (headerCount == 4) {
                        int length = ByteBuffer.wrap(header).getInt();
                        if (length < 69 || length > config.maxFrameBytes()) {
                            context.close();
                            return;
                        }
                        total = length + 4;
                        if (!input.isReadable()) return;
                        continue;
                    }
                    if (headerCount == 8) {
                        short version = ByteBuffer.wrap(header).getShort(6);
                        if (version != 1 && version != 2 || total < headerBytes() + 4) {
                            context.close();
                            return;
                        }
                        if (!input.isReadable()) return;
                        continue;
                    }
                    if (headerCount < headerBytes()) return;
                    if (!validHeader()) {
                        context.close();
                        return;
                    }
                    lease = (peerControl ? control : shared).reserve(total).orElse(null);
                    if (lease == null && peerControl) lease = shared.reserve(total).orElse(null);
                    if (lease == null) {
                        context.close();
                        return;
                    }
                    bytes = new byte[total];
                    System.arraycopy(header, 0, bytes, 0, headerBytes());
                    count = headerBytes();
                }
                int copy = Math.min(bytes.length - count, input.readableBytes());
                input.readBytes(bytes, count, copy);
                count += copy;
                if (count == bytes.length) {
                    if (deadline != null) {
                        deadline.cancel();
                        deadline = null;
                    }
                    var complete = new OwnedFrame(bytes, lease, peerControl);
                    bytes = null;
                    lease = null;
                    headerCount = 0;
                    count = 0;
                    context.fireChannelRead(complete);
                }
            }
        } finally {
            input.release();
        }
    }

    // Admin (sender -1) may only send requests for DescribeQuorum and the admin operations. A voter
    // may not send admin-operation requests. Only peer control operations up to 64 KiB, excluding
    // FetchSnapshot, may use the control pool; snapshot frames always use the shared pool.
    private boolean validHeader() {
        var in = ByteBuffer.wrap(header);
        in.getInt();
        int op = in.getShort(), version = in.getShort(), direction = in.get();
        UUID cluster = new UUID(in.getLong(), in.getLong());
        int sender = in.getInt();
        if (version == 2) {
            int role = in.get() & 255;
            if (role > 2 || direction < 0 || direction > 1) return false;
            var senderRole = BrokerControlProtocol.SenderRole.values()[role];
            long request = in.getLong();
            byte[] hash = new byte[32];
            in.get(hash);
            if (request < 0
                    || !cluster.equals(config.identity().clusterId())
                    || !BrokerControlProtocol.allowed(
                            (short) version, senderRole, (short) op, direction == 1)) return false;
            if (senderRole == BrokerControlProtocol.SenderRole.VOTER) {
                if (config.identity().voters().stream().noneMatch(v -> v.id() == sender)
                        || !Arrays.equals(hash, config.identity().voterHash())) return false;
            } else if (senderRole == BrokerControlProtocol.SenderRole.ADMIN
                    ? sender != -1
                    : sender < 0) return false;
            else if (!Arrays.equals(hash, new byte[32])) return false;
            peerControl =
                    senderRole == BrokerControlProtocol.SenderRole.VOTER
                            && op <= 106
                            && op != 105
                            && total <= 64 * 1024;
            return true;
        }
        long request = in.getLong();
        byte[] hash = new byte[32];
        in.get(hash);
        if (version != 1
                || direction < 0
                || direction > 1
                || request < 0
                || !cluster.equals(config.identity().clusterId())
                || !Arrays.equals(hash, config.identity().voterHash())
                || op < 101
                || op > 109) return false;
        if (sender == -1) {
            if (direction != 0 || op < 106) return false;
        } else if (config.identity().voters().stream().noneMatch(v -> v.id() == sender)
                || direction == 0 && op > 106) return false;
        peerControl = sender >= 0 && op <= 106 && op != 105 && total <= 64 * 1024;
        return true;
    }

    private int headerBytes() {
        return ByteBuffer.wrap(header).getShort(6) == 2 ? 70 : 69;
    }

    private void release() {
        if (lease != null) lease.close();
        lease = null;
        bytes = null;
        if (deadline != null) deadline.cancel();
        deadline = null;
        headerCount = 0;
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        release();
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext context) {
        release();
    }

    /**
     * One complete raw frame, including its length prefix, and the budget lease charged for it. The
     * lease is held until {@link #close}.
     */
    public static final class OwnedFrame implements AutoCloseable {
        private final byte[] bytes;
        private final ResourceBudget.Lease lease;
        private final boolean control;

        private OwnedFrame(byte[] bytes, ResourceBudget.Lease lease, boolean control) {
            this.bytes = bytes;
            this.lease = lease;
            this.control = control;
        }

        public byte[] bytes() {
            return bytes;
        }

        public boolean control() {
            return control;
        }

        @Override
        public void close() {
            lease.close();
        }
    }
}
