package vn.huyqt.logbroker.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.ProtocolLimits;

/**
 * Incremental length framing with a reservation before body allocation.
 *
 * <p>Reads a big-endian {@code int32} length prefix, rejects lengths outside {@code
 * [minFrameBytes, maxFrameBytes]}, and reserves {@code length + 4} bytes from the shared budget
 * before allocating the body. Any violation or an exhausted budget closes the connection, as
 * malformed framing does in {@code docs/protocol-v1.md}. Partial and coalesced reads are both
 * handled. Each complete frame is copied into a heap array and passed on as an
 * {@link OwnedFrame}, so no pooled Netty buffer escapes the event loop.
 *
 * <p>Not {@code @Sharable}: one instance per channel, confined to its event loop.
 */
public final class BoundedFrameDecoder extends ChannelInboundHandlerAdapter {
  private final int minFrameBytes, maxFrameBytes;
  private final ResourceBudget budget;
  private final DeadlineScheduler clock;
  private final byte[] prefix = new byte[4];
  private int prefixCount;
  private byte[] frame;
  private int frameCount;
  private ResourceBudget.Lease lease;
  private DeadlineScheduler.Ticket frameDeadline;

  /** Broker protocol framing without a per-frame deadline, as used by the client transport. */
  public BoundedFrameDecoder(ProtocolLimits limits, ResourceBudget budget) {
    this(limits, budget, null);
  }

  /**
   * Broker protocol framing: the minimum of 12 bytes is the envelope header (operation, version,
   * request ID) from {@code docs/protocol-v1.md}.
   *
   * @param clock if non-null, closes the connection when a frame is not complete within 30
   *     seconds of its first byte
   */
  public BoundedFrameDecoder(
      ProtocolLimits limits, ResourceBudget budget, DeadlineScheduler clock) {
    this(12, Objects.requireNonNull(limits).maxFrameBytes(), budget, clock);
  }

  /**
   * Framing with explicit bounds, independent of the broker protocol header.
   *
   * @param minFrameBytes smallest accepted length after the prefix
   * @param maxFrameBytes largest accepted length after the prefix; bounded so that
   *     {@code length + 4} cannot overflow
   * @param budget shared reservation for complete and partial frames; a frame holds its lease
   *     until its {@link OwnedFrame} is closed
   * @param clock optional per-frame deadline, see
   *     {@link #BoundedFrameDecoder(ProtocolLimits, ResourceBudget, DeadlineScheduler)}
   */
  public BoundedFrameDecoder(
      int minFrameBytes, int maxFrameBytes, ResourceBudget budget, DeadlineScheduler clock) {
    if (minFrameBytes < 1 || maxFrameBytes < minFrameBytes || maxFrameBytes > Integer.MAX_VALUE - 4)
      throw new IllegalArgumentException("Invalid frame lengths");
    this.minFrameBytes = minFrameBytes;
    this.maxFrameBytes = maxFrameBytes;
    this.budget = Objects.requireNonNull(budget);
    this.clock = clock;
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
        if (frame == null) {
          // The deadline starts at the first prefix byte so a stalled sender cannot hold a
          // partial frame and its reservation indefinitely.
          if (prefixCount == 0 && clock != null)
            frameDeadline =
                clock.schedule(clock.nanoTime() + Duration.ofSeconds(30).toNanos(), context::close);
          int copy = Math.min(4 - prefixCount, input.readableBytes());
          input.readBytes(prefix, prefixCount, copy);
          prefixCount += copy;
          if (prefixCount < 4) return;
          int length = ByteBuffer.wrap(prefix).getInt();
          if (length < minFrameBytes || length > maxFrameBytes) {
            context.close();
            return;
          }
          // Reserve before allocating so a peer-supplied length cannot force an allocation
          // beyond the shared budget.
          lease = budget.reserve((long) length + 4).orElse(null);
          if (lease == null) {
            context.close();
            return;
          }
          frame = new byte[length + 4];
          System.arraycopy(prefix, 0, frame, 0, 4);
          frameCount = 4;
        }
        int copy = Math.min(frame.length - frameCount, input.readableBytes());
        input.readBytes(frame, frameCount, copy);
        frameCount += copy;
        if (frameCount == frame.length) {
          if (frameDeadline != null) {
            frameDeadline.cancel();
            frameDeadline = null;
          }
          OwnedFrame complete = new OwnedFrame(frame, lease);
          // Lease ownership moves to the frame; clear local state first so releasePartial
          // cannot return the reservation while the receiver still holds the frame.
          frame = null;
          frameCount = 0;
          prefixCount = 0;
          lease = null;
          context.fireChannelRead(complete);
        }
      }
    } finally {
      // Bytes were copied out, so this handler is the last user of the inbound buffer.
      input.release();
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    releasePartial();
    super.channelInactive(context);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext context) throws Exception {
    releasePartial();
    super.handlerRemoved(context);
  }

  // Runs on both close and handler removal so a partial frame never leaks its reservation or
  // leaves its deadline armed.
  private void releasePartial() {
    if (frameDeadline != null) {
      frameDeadline.cancel();
      frameDeadline = null;
    }
    if (lease != null) lease.close();
    lease = null;
    frame = null;
  }

  /**
   * A complete frame in a heap array that the receiver owns. {@link #bytes()} includes the
   * 4-byte length prefix. The receiver must {@link #close()} it to return the reservation;
   * closing more than once has no further effect.
   */
  public static final class OwnedFrame implements AutoCloseable {
    private final byte[] bytes;
    private final ResourceBudget.Lease lease;

    private OwnedFrame(byte[] bytes, ResourceBudget.Lease lease) {
      this.bytes = bytes;
      this.lease = lease;
    }

    public byte[] bytes() {
      return bytes;
    }

    @Override
    public void close() {
      lease.close();
    }
  }
}
