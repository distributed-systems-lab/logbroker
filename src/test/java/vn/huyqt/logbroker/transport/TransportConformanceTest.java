package vn.huyqt.logbroker.transport;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.LegacyBrokerFixture;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolLimits;
import vn.huyqt.logbroker.transport.netty.NettyClientTransport;

import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Shared transport contract for a future standard-library adapter. */
class TransportConformanceTest {
    @TempDir Path directory;

    @Test
    void nettyAdapterObeysConnectionAndFrameContract() throws Exception {
        runContract(() -> new NettyClientTransport(ProtocolLimits.defaults()));
    }

    private void runContract(Supplier<ClientTransport> factory) throws Exception {
        try (var broker = LegacyBrokerFixture.start(BrokerConfig.defaults(directory).withPort(0));
                var transport = factory.get()) {
            var replies = new LinkedBlockingQueue<Protocol.ResponseFrame>();
            var failures = new LinkedBlockingQueue<Throwable>();
            transport
                    .connect(broker.address(), replies::add, failures::add)
                    .get(5, TimeUnit.SECONDS);
            transport
                    .send(
                            new Protocol.RequestFrame(
                                    (short) 1,
                                    (short) 1,
                                    41,
                                    new Protocol.CreateTopic("contract", 1)))
                    .get(5, TimeUnit.SECONDS);
            var reply = replies.poll(5, TimeUnit.SECONDS);
            assertNotNull(reply);
            assertEquals(41, reply.requestId());
            assertEquals(ErrorCode.NONE, ((Protocol.CreateTopicReply) reply.body()).error().code());
            assertTrue(failures.isEmpty());
        }
    }
}
