package vn.huyqt.logbroker.controller.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;

class QuorumTransportTest {
  @Test
  void thirtyThirdInflightRequestClosesConnectionButRetainedConsumerLeaseSurvivesDisconnect()
      throws Exception {
    var config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
    var received = new LinkedBlockingQueue<QuorumTransport.Inbound>();
    var failures = new LinkedBlockingQueue<Throwable>();
    try (var clock = DeadlineScheduler.system()) {
      var transport = new NettyQuorumTransport(config, clock);
      var address =
          transport.start(new InetSocketAddress("127.0.0.1", 0), received::add, failures::add);
      var held = new java.util.ArrayList<QuorumTransport.Inbound>();
      try (var socket = new Socket(address.getAddress(), address.getPort())) {
        socket.setSoTimeout(3000);
        var identity = config.identity();
        for (int id = 0; id < 32; id++) {
          socket
              .getOutputStream()
              .write(
                  QuorumCodec.encode(
                      new Frame(
                          (short) 106,
                          false,
                          identity.clusterId(),
                          -1,
                          id,
                          identity.voterHash(),
                          new DescribeQuorum())));
          var inbound = received.poll(3, TimeUnit.SECONDS);
          assertNotNull(inbound);
          held.add(inbound);
        }
        socket
            .getOutputStream()
            .write(
                QuorumCodec.encode(
                    new Frame(
                        (short) 106,
                        false,
                        identity.clusterId(),
                        -1,
                        32,
                        identity.voterHash(),
                        new DescribeQuorum())));
        assertEquals(-1, socket.getInputStream().read());
        assertNotNull(failures.poll(3, TimeUnit.SECONDS));
        assertTrue(transport.inboundUsed() > 0);
      } finally {
        for (var inbound : held) inbound.close();
        transport.closeAsync().get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, transport.inboundUsed());
      assertEquals(0, transport.connectionsUsed());
    }
  }

  @Test
  void bindFailureClosesTransportInsteadOfLeavingWorkerGroupsAlive() throws Exception {
    var config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
    try (var clock = DeadlineScheduler.system();
        var occupied = new java.net.ServerSocket(0)) {
      var transport = new NettyQuorumTransport(config, clock);
      try {
        assertThrows(
            IOException.class,
            () ->
                transport.start(
                    new InetSocketAddress("127.0.0.1", occupied.getLocalPort()),
                    inbound -> inbound.close(),
                    ignored -> {}));
        assertThrows(
            IllegalStateException.class,
            () ->
                transport.start(
                    new InetSocketAddress("127.0.0.1", 0),
                    inbound -> inbound.close(),
                    ignored -> {}));
      } finally {
        transport.closeAsync().get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void disconnectedRouteCannotReplyToNewConnectionWithReusedRequestId() throws Exception {
    var config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
    var received = new LinkedBlockingQueue<QuorumTransport.Inbound>();
    var failures = new LinkedBlockingQueue<Throwable>();
    try (var clock = DeadlineScheduler.system()) {
      var transport = new NettyQuorumTransport(config, clock);
      var address =
          transport.start(new InetSocketAddress("127.0.0.1", 0), received::add, failures::add);
      var identity = config.identity();
      var request =
          new Frame(
              (short) 106,
              false,
              identity.clusterId(),
              -1,
              7,
              identity.voterHash(),
              new DescribeQuorum());
      QuorumTransport.Inbound first;
      try (var old = new Socket(address.getAddress(), address.getPort())) {
        old.getOutputStream().write(QuorumCodec.encode(request));
        first = received.poll(3, TimeUnit.SECONDS);
        assertNotNull(first);
      }
      assertNotNull(failures.poll(3, TimeUnit.SECONDS));
      try (var current = new Socket(address.getAddress(), address.getPort())) {
        current.getOutputStream().write(QuorumCodec.encode(request));
        var second = received.poll(3, TimeUnit.SECONDS);
        assertNotNull(second);
        assertNotEquals(first.route().connectionId(), second.route().connectionId());
        var reply =
            new Frame(
                (short) 106,
                true,
                identity.clusterId(),
                0,
                7,
                identity.voterHash(),
                new Failure(new ReplyMeta(QuorumError.NODE_UNAVAILABLE, "", 0, -1)));
        assertThrows(
            ExecutionException.class,
            () -> transport.reply(first.route(), reply).get(3, TimeUnit.SECONDS));
        first.close();
        second.close();
      } finally {
        transport.closeAsync().get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, transport.inboundUsed());
    }
  }

  @Test
  void peerRequestsAndResponsesUseRealOutgoingConnection() throws Exception {
    int onePort;
    try (var unused = new java.net.ServerSocket(0)) {
      onePort = unused.getLocalPort();
    }
    var voters =
        java.util.List.of(
            new ClusterIdentity.Voter(0, "127.0.0.1", 19090),
            new ClusterIdentity.Voter(1, "127.0.0.1", onePort),
            new ClusterIdentity.Voter(2, "127.0.0.1", 19092));
    var zero = ControllerConfig.defaults(new ClusterIdentity(new java.util.UUID(0, 1), 0, voters));
    var one = ControllerConfig.defaults(new ClusterIdentity(new java.util.UUID(0, 1), 1, voters));
    var responses = new LinkedBlockingQueue<QuorumTransport.Inbound>();
    try (var clock = DeadlineScheduler.system()) {
      var server = new NettyQuorumTransport(one, clock);
      var client = new NettyQuorumTransport(zero, clock);
      server.start(
          new InetSocketAddress("127.0.0.1", onePort),
          inbound -> {
            server.reply(
                inbound.route(),
                new Frame(
                    (short) 101,
                    true,
                    one.identity().clusterId(),
                    1,
                    inbound.frame().requestId(),
                    one.identity().voterHash(),
                    new VoteReply(new ReplyMeta(QuorumError.NONE, "", 1, -1), true)));
            inbound.close();
          },
          ignored -> {});
      client.start(new InetSocketAddress("127.0.0.1", 0), responses::add, ignored -> {});
      try {
        client
            .send(
                1,
                new Frame(
                    (short) 101,
                    false,
                    zero.identity().clusterId(),
                    0,
                    1,
                    zero.identity().voterHash(),
                    new Vote(1, 0, 0)))
            .get(3, TimeUnit.SECONDS);
        var reply = responses.poll(3, TimeUnit.SECONDS);
        assertNotNull(reply);
        assertTrue(((VoteReply) reply.frame().message()).granted());
        reply.close();
      } finally {
        client.closeAsync().get(5, TimeUnit.SECONDS);
        server.closeAsync().get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, client.inboundUsed() + server.inboundUsed());
      assertEquals(0, client.outboundUsed() + server.outboundUsed());
    }
  }

  @Test
  void coalescedRequestsReplyByConnectionIncarnationAndRequestId() throws Exception {
    var config = ControllerConfig.defaults(ControllerTestSupport.identity(0));
    var received = new LinkedBlockingQueue<QuorumTransport.Inbound>();
    try (var clock = DeadlineScheduler.system()) {
      var transport = new NettyQuorumTransport(config, clock);
      var address =
          transport.start(new InetSocketAddress("127.0.0.1", 0), received::add, ignored -> {});
      try (var socket = new Socket()) {
        socket.connect(address);
        socket.setSoTimeout(3000);
        var identity = config.identity();
        for (int id = 1; id <= 2; id++)
          socket
              .getOutputStream()
              .write(
                  QuorumCodec.encode(
                      new Frame(
                          (short) 106,
                          false,
                          identity.clusterId(),
                          -1,
                          id,
                          identity.voterHash(),
                          new DescribeQuorum())));
        var first = received.poll(3, TimeUnit.SECONDS);
        var second = received.poll(3, TimeUnit.SECONDS);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(first.route().connectionId(), second.route().connectionId());
        var reply = new Failure(new ReplyMeta(QuorumError.NODE_UNAVAILABLE, "test", 0, -1));
        transport
            .reply(
                second.route(),
                new Frame(
                    (short) 106,
                    true,
                    identity.clusterId(),
                    0,
                    second.frame().requestId(),
                    identity.voterHash(),
                    reply))
            .get(3, TimeUnit.SECONDS);
        transport
            .reply(
                first.route(),
                new Frame(
                    (short) 106,
                    true,
                    identity.clusterId(),
                    0,
                    first.frame().requestId(),
                    identity.voterHash(),
                    reply))
            .get(3, TimeUnit.SECONDS);
        assertEquals(2, read(socket, config).requestId());
        assertEquals(1, read(socket, config).requestId());
        first.close();
        second.close();
      } finally {
        transport.closeAsync().get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, transport.inboundUsed());
      assertEquals(0, transport.outboundUsed());
    }
  }

  static Frame read(Socket socket, ControllerConfig config) throws Exception {
    var input = new DataInputStream(socket.getInputStream());
    int length = input.readInt();
    byte[] bytes = new byte[length + 4];
    ByteBuffer.wrap(bytes).putInt(length);
    input.readFully(bytes, 4, length);
    return QuorumCodec.decode(bytes, config);
  }
}
