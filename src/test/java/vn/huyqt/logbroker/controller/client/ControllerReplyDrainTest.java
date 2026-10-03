package vn.huyqt.logbroker.controller.client;

import static org.junit.jupiter.api.Assertions.*;
import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.broker.cluster.BrokerClusterConfig;
import vn.huyqt.logbroker.controller.metadata.ClusterRecords.Endpoint;
import vn.huyqt.logbroker.controller.metadata.MetadataLimits;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

class ControllerReplyDrainTest {
    @Test void disconnectWaitsForAlreadyReceivedReplyDecode() throws Exception {
        var release = new CountDownLatch(1);
        try (var server = new ServerSocket(0); var clock = DeadlineScheduler.system()) {
            var config = new BrokerClusterConfig(new UUID(0, 1), 7, new Endpoint("localhost", 9092),
                    List.of(new Endpoint("localhost", server.getLocalPort())), Duration.ofSeconds(1),
                    Duration.ofSeconds(2), Duration.ofSeconds(10), MetadataLimits.defaults());
            try (var factory = new NettyControllerClientTransport.Factory(config, clock)) {
                var field = factory.getClass().getDeclaredField("decode");
                field.setAccessible(true);
                var decode = (ThreadPoolExecutor) field.get(factory);
                var blocked = new CountDownLatch(2);
                for (int i = 0; i < 2; i++) decode.execute(() -> {
                    blocked.countDown();
                    try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
                assertTrue(blocked.await(2, TimeUnit.SECONDS));
                var reply = new CompletableFuture<Frame>();
                var failure = new CompletableFuture<Throwable>();
                var overtook = new java.util.concurrent.atomic.AtomicBoolean();
                var peer = CompletableFuture.runAsync(() -> {
                    try (var socket = server.accept()) {
                        var input = new DataInputStream(socket.getInputStream());
                        input.readNBytes(input.readInt());
                        socket.getOutputStream().write(QuorumCodec.encode(new Frame((short) 2,
                                BrokerControlProtocol.SenderRole.VOTER, (short) 106, true,
                                config.clusterId(), 1, 1, new byte[32],
                                new Failure(new ReplyMeta(QuorumError.NODE_UNAVAILABLE, "drain", 1, 1)))));
                    } catch (Exception e) { throw new CompletionException(e); }
                });
                var transport = factory.create();
                transport.connect(new InetSocketAddress("localhost", server.getLocalPort()), reply::complete,
                        error -> { overtook.set(!reply.isDone()); failure.complete(error); })
                        .get(2, TimeUnit.SECONDS);
                transport.send(new Frame((short) 2, BrokerControlProtocol.SenderRole.BROKER, (short) 106,
                        false, config.clusterId(), 7, 1, new byte[32], new DescribeQuorum())).get(2, TimeUnit.SECONDS);
                peer.get(2, TimeUnit.SECONDS);
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (decode.getQueue().isEmpty() && System.nanoTime() < until) Thread.sleep(5);
                assertFalse(decode.getQueue().isEmpty());
                assertThrows(TimeoutException.class, () -> failure.get(200, TimeUnit.MILLISECONDS));
                release.countDown();
                assertInstanceOf(Failure.class, reply.get(2, TimeUnit.SECONDS).message());
                failure.get(2, TimeUnit.SECONDS);
                assertFalse(overtook.get(), "Disconnect overtook a received frame");
            } finally { release.countDown(); }
        }
    }
}
