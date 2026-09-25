package vn.huyqt.logbroker.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.Objects;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.ProtocolLimits;

/** Incremental length framing with a reservation before body allocation. */
public final class BoundedFrameDecoder extends ChannelInboundHandlerAdapter {
    private final ProtocolLimits limits;
    private final ResourceBudget budget;
    private final byte[] prefix = new byte[4];
    private int prefixCount;
    private byte[] frame;
    private int frameCount;
    private ResourceBudget.Lease lease;

    public BoundedFrameDecoder(ProtocolLimits limits, ResourceBudget budget) {
        this.limits = Objects.requireNonNull(limits);
        this.budget = Objects.requireNonNull(budget);
    }

    @Override public void channelRead(ChannelHandlerContext context, Object message) {
        if (!(message instanceof ByteBuf input)) {
            ReferenceCountUtil.release(message);
            context.close();
            return;
        }
        try {
            while (input.isReadable() && context.channel().isOpen()) {
                if (frame == null) {
                    int copy = Math.min(4 - prefixCount, input.readableBytes());
                    input.readBytes(prefix, prefixCount, copy);
                    prefixCount += copy;
                    if (prefixCount < 4) return;
                    int length = ByteBuffer.wrap(prefix).getInt();
                    if (length < 12 || length > limits.maxFrameBytes()) {
                        context.close();
                        return;
                    }
                    lease = budget.reserve((long) length + 4).orElse(null);
                    if (lease == null) { context.close(); return; }
                    frame = new byte[length + 4];
                    System.arraycopy(prefix, 0, frame, 0, 4);
                    frameCount = 4;
                }
                int copy = Math.min(frame.length - frameCount, input.readableBytes());
                input.readBytes(frame, frameCount, copy);
                frameCount += copy;
                if (frameCount == frame.length) {
                    OwnedFrame complete = new OwnedFrame(frame, lease);
                    frame = null;
                    frameCount = 0;
                    prefixCount = 0;
                    lease = null;
                    context.fireChannelRead(complete);
                }
            }
        } finally {
            input.release();
        }
    }

    @Override public void channelInactive(ChannelHandlerContext context) throws Exception {
        releasePartial();
        super.channelInactive(context);
    }

    @Override public void handlerRemoved(ChannelHandlerContext context) throws Exception {
        releasePartial();
        super.handlerRemoved(context);
    }

    private void releasePartial() {
        if (lease != null) lease.close();
        lease = null;
        frame = null;
    }

    public static final class OwnedFrame implements AutoCloseable {
        private final byte[] bytes;
        private final ResourceBudget.Lease lease;

        private OwnedFrame(byte[] bytes, ResourceBudget.Lease lease) {
            this.bytes = bytes; this.lease = lease;
        }

        public byte[] bytes() { return bytes; }
        @Override public void close() { lease.close(); }
    }
}
