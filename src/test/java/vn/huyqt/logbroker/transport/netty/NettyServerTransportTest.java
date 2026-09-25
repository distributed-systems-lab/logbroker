package vn.huyqt.logbroker.transport.netty;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.broker.metadata.MetadataService;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolCodec;
import vn.huyqt.logbroker.protocol.ErrorCode;
import vn.huyqt.logbroker.storage.LogRecord;
import vn.huyqt.logbroker.storage.RecordHeader;

class NettyServerTransportTest {
    @TempDir Path directory;

    @Test
    void servesCreateTopicOverRealTcpAndReturnsCorrelatedResponse() throws Exception {
        var config = BrokerConfig.defaults(directory).withPort(0);
        var codec = new ProtocolCodec(config.protocolLimits());
        try (var clock = DeadlineScheduler.system();
             var registry = new PartitionRegistry(config, FilePartitionStore::open);
             var metadata = MetadataService.open(directory, config, registry)) {
            var fetch = new FetchCoordinator(new FetchPlanner(Map.of(), config),
                    ignored -> null, clock, new ResourceBudget(8));
            var dispatcher = new RequestDispatcher(metadata, ignored -> null, fetch, clock);
            var server = new NettyServerTransport(config, clock);
            try {
                InetSocketAddress bound = server.start(new InetSocketAddress("127.0.0.1", 0),
                        dispatcher);
                try (var socket = new Socket(bound.getAddress(), bound.getPort())) {
                    socket.setSoTimeout(5000);
                    var request = new Protocol.RequestFrame((short) 1, (short) 1, 91,
                            new Protocol.CreateTopic("orders", 1));
                    socket.getOutputStream().write(codec.encodeRequest(request));
                    var input = new DataInputStream(socket.getInputStream());
                    int size = input.readInt();
                    assertTrue(size > 12 && size < config.protocolLimits().maxFrameBytes());
                    byte[] response = new byte[size + 4];
                    java.nio.ByteBuffer.wrap(response).putInt(size);
                    input.readFully(response, 4, size);
                    var decoded = codec.decodeResponse(response);
                    assertEquals(91, decoded.requestId());
                    var reply = (Protocol.CreateTopicReply) decoded.body();
                    assertEquals(ErrorCode.NONE, reply.error().code());
                }
            } finally {
                server.closeAsync().get();
                fetch.close();
            }
        }
    }

    @Test
    void longPollRetainsDecodedRequestBudgetUntilResponse() throws Exception {
        var config = BrokerConfig.defaults(directory).withPort(0);
        var codec = new ProtocolCodec(config.protocolLimits());
        var inputBudget = new ResourceBudget(config.maxQueuedRequestBytes());
        var waiterBudget = new ResourceBudget(8);
        try (var clock = DeadlineScheduler.system();
             var workers = new PartitionExecutor(1, 1, 8);
             var registry = new PartitionRegistry(config, FilePartitionStore::open);
             var metadata = MetadataService.open(directory, config, registry)) {
            UUID id = metadata.create("orders", 1).get().topicId();
            var tp = new Protocol.TopicPartition(id, 0);
            var runtime = new PartitionRuntime(tp, registry.require(tp), workers, clock, config);
            var runtimes = Map.of(tp, runtime);
            var fetch = new FetchCoordinator(new FetchPlanner(runtimes, config),
                    runtimes::get, clock, waiterBudget);
            var dispatcher = new RequestDispatcher(metadata, runtimes::get, fetch, clock);
            var server = new NettyServerTransport(config, clock, inputBudget);
            try {
                var bound = server.start(new InetSocketAddress("127.0.0.1", 0), dispatcher);
                try (var socket = new Socket(bound.getAddress(), bound.getPort())) {
                    socket.setSoTimeout(5000);
                    var request = new Protocol.RequestFrame((short) 4, (short) 1, 92,
                            new Protocol.Fetch(1024, 1, 5000,
                                    List.of(new Protocol.FetchEntry(tp, 0, 1024))));
                    byte[] fetchFrame = codec.encodeRequest(request);
                    socket.getOutputStream().write(fetchFrame);
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                    while (waiterBudget.used() == 0 && System.nanoTime() < until)
                        Thread.sleep(5);
                    assertEquals(1, waiterBudget.used());
                    socket.getOutputStream().write(codec.encodeRequest(new Protocol.RequestFrame(
                            (short) 2, (short) 1, 93, new Protocol.Metadata(List.of("orders")))));
                    var input = new DataInputStream(socket.getInputStream());
                    int size = input.readInt();
                    byte[] reply = new byte[size + 4];
                    java.nio.ByteBuffer.wrap(reply).putInt(size);
                    input.readFully(reply, 4, size);
                    assertEquals(93, codec.decodeResponse(reply).requestId());
                    long retained = 2L * fetchFrame.length;
                    until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                    while (inputBudget.used() != retained && System.nanoTime() < until)
                        Thread.sleep(5);
                    assertEquals(retained, inputBudget.used(),
                            "Long poll retains its decoded request after later validation completes");
                }
            } finally {
                server.closeAsync().get();
                fetch.close(); runtime.close();
                assertEquals(0, inputBudget.used());
                assertEquals(0, waiterBudget.used());
            }
        }
    }

    @Test
    void amplifiedHeadersAreRejectedBeforeDecodedObjectAllocation() throws Exception {
        var config = BrokerConfig.defaults(directory).withPort(0);
        var codec = new ProtocolCodec(config.protocolLimits());
        var headers = java.util.Collections.nCopies(50, new RecordHeader("", null));
        var tp = new Protocol.TopicPartition(UUID.randomUUID(), 0);
        var frame = codec.encodeRequest(new Protocol.RequestFrame((short) 3, (short) 1,
                94, new Protocol.Produce(Protocol.AckMode.APPENDED, 1000,
                List.of(new Protocol.ProduceEntry(tp,
                        new Protocol.Batch(List.of(new LogRecord(0, null, null, headers))))))));
        var budget = new ResourceBudget(3L * frame.length);
        try (var clock = DeadlineScheduler.system();
             var registry = new PartitionRegistry(config, FilePartitionStore::open);
             var metadata = MetadataService.open(directory, config, registry)) {
            var fetch = new FetchCoordinator(new FetchPlanner(Map.of(), config),
                    ignored -> null, clock, new ResourceBudget(8));
            var dispatcher = new RequestDispatcher(metadata, ignored -> null, fetch, clock);
            var server = new NettyServerTransport(config, clock, budget);
            try {
                var bound = server.start(new InetSocketAddress("127.0.0.1", 0), dispatcher);
                try (var socket = new Socket(bound.getAddress(), bound.getPort())) {
                    socket.setSoTimeout(5000);
                    socket.getOutputStream().write(frame);
                    assertEquals(-1, socket.getInputStream().read());
                }
            } finally {
                server.closeAsync().get();
                fetch.close();
                assertEquals(0, budget.used());
            }
        }
    }
}
