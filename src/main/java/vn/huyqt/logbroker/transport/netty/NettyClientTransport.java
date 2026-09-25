package vn.huyqt.logbroker.transport.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import vn.huyqt.logbroker.broker.ResourceBudget;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolCodec;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.transport.ClientTransport;

/** Netty framing is confined to this transport adapter. */
public final class NettyClientTransport implements ClientTransport {
    private final ProtocolLimits limits;
    private final ProtocolCodec codec;
    private final ResourceBudget inbound;
    private final MultiThreadIoEventLoopGroup loops =
            new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private volatile Channel channel;
    private volatile boolean closed;

    public NettyClientTransport(ProtocolLimits limits) {
        this.limits = Objects.requireNonNull(limits);
        codec = new ProtocolCodec(limits);
        inbound = new ResourceBudget(32L * (limits.maxFrameBytes() + 4L));
    }

    @Override public synchronized CompletableFuture<Void> connect(InetSocketAddress address,
            Consumer<Protocol.ResponseFrame> response, Consumer<Throwable> failure) {
        if (closed) return CompletableFuture.failedFuture(new IOException("Transport closed"));
        var ready = new CompletableFuture<Void>();
        var old = channel;
        if (old != null) old.close();
        var bootstrap = new Bootstrap().group(loops).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel socket) {
                        socket.pipeline().addLast(new BoundedFrameDecoder(limits, inbound));
                        socket.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            @Override public void channelRead(ChannelHandlerContext context,
                                                              Object message) {
                                if (!(message instanceof BoundedFrameDecoder.OwnedFrame frame)) {
                                    context.close(); return;
                                }
                                try (frame) { response.accept(codec.decodeResponse(frame.bytes())); }
                                catch (Throwable error) { failure.accept(error); context.close(); }
                            }

                            @Override public void channelInactive(ChannelHandlerContext context) {
                                failure.accept(new IOException("Broker connection closed"));
                            }

                            @Override public void exceptionCaught(ChannelHandlerContext context,
                                                                  Throwable error) {
                                failure.accept(error);
                                context.close();
                            }
                        });
                    }
                });
        bootstrap.connect(address).addListener(result -> {
            if (result.isSuccess()) {
                channel = ((io.netty.channel.ChannelFuture) result).channel();
                ready.complete(null);
            } else ready.completeExceptionally(result.cause());
        });
        return ready;
    }

    @Override public CompletableFuture<Void> send(Protocol.RequestFrame request) {
        final byte[] bytes;
        try { bytes = codec.encodeRequest(request); }
        catch (Exception error) { return CompletableFuture.failedFuture(error); }
        var current = channel;
        if (current == null || !current.isActive())
            return CompletableFuture.failedFuture(new IOException("Broker not connected"));
        var written = new CompletableFuture<Void>();
        current.writeAndFlush(Unpooled.wrappedBuffer(bytes)).addListener(result -> {
            if (result.isSuccess()) written.complete(null);
            else written.completeExceptionally(result.cause());
        });
        return written;
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        if (channel != null) channel.close();
        loops.shutdownGracefully();
    }
}
