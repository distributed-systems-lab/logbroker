package vn.huyqt.logbroker.client;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolCodec;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

class NettyClientLoopbackTest {
    @Test void sendsAndReceivesCorrelatedFrameOverTcp() throws Exception {
        var codec = new ProtocolCodec(ProtocolLimits.defaults());
        try (var listener = new ServerSocket(0)) {
            var server = CompletableFuture.runAsync(() -> {
                try (var socket = listener.accept()) {
                    var input = new DataInputStream(socket.getInputStream());
                    int length = input.readInt();
                    byte[] request = new byte[length + 4];
                    java.nio.ByteBuffer.wrap(request).putInt(length);
                    input.readFully(request, 4, length);
                    long id = codec.decodeRequest(request).requestId();
                    byte[] reply = codec.encodeResponse(new Protocol.ResponseFrame(
                            (short) 1, (short) 1, id, new Protocol.CreateTopicReply(
                                    Protocol.Error.none(), UUID.randomUUID())));
                    socket.getOutputStream().write(reply, 0, 2);
                    socket.getOutputStream().write(reply, 2, reply.length - 2);
                } catch (Exception error) { throw new RuntimeException(error); }
            });
            var config = ClientConfig.defaults(new InetSocketAddress("127.0.0.1",
                    listener.getLocalPort()));
            try (var clock = DeadlineScheduler.system();
                 var client = new BrokerClient(config, new NettyClientTransport(
                         ProtocolLimits.defaults()), clock)) {
                var result = client.request(new Protocol.CreateTopic("orders", 1))
                        .get(5, TimeUnit.SECONDS);
                assertEquals(Protocol.Error.none(),
                        ((Protocol.CreateTopicReply) result).error());
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }
}
