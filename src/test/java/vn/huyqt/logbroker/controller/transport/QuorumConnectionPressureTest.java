package vn.huyqt.logbroker.controller.transport;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.channel.embedded.EmbeddedChannel;
import java.net.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.*;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.support.ManualScheduler;

class QuorumConnectionPressureTest {
  @Test
  void noByteConnectionHasBoundedHandshakeLifetime() {
    var id = vn.huyqt.logbroker.controller.support.ControllerTestSupport.identity(0);
    var clock = new ManualScheduler();
    var channel =
        new EmbeddedChannel(
            new QuorumFrameDecoder(
                ControllerConfig.defaults(id),
                new ResourceBudget(1024),
                new ResourceBudget(1024),
                clock));
    clock.advance(Duration.ofSeconds(31));
    clock.runDue();
    channel.runPendingTasks();
    assertFalse(channel.isOpen(), "Idle socket has no deadline");
    channel.finishAndReleaseAll();
  }

  @Test
  void idleSocketsCannotPreventPeerEstablishmentOrReconnect() throws Exception {
    var voters = new ArrayList<ClusterIdentity.Voter>();
    for (int i = 0; i < 3; i++)
      try (var socket = new ServerSocket(0)) {
        voters.add(new ClusterIdentity.Voter(i, "127.0.0.1", socket.getLocalPort()));
      }
    var cluster = UUID.randomUUID();
    var id0 = new ClusterIdentity(cluster, 0, voters);
    var id1 = new ClusterIdentity(cluster, 1, voters);
    var frames = new LinkedBlockingQueue<Frame>();
    var idle = new ArrayList<Socket>();
    try (var clock = DeadlineScheduler.system()) {
      var first = new NettyQuorumTransport(ControllerConfig.defaults(id0), clock);
      var second = new NettyQuorumTransport(ControllerConfig.defaults(id1), clock);
      try {
        first.start(
            new InetSocketAddress("127.0.0.1", voters.get(0).port()),
            inbound -> {
              frames.add(inbound.frame());
              inbound.close();
            },
            ignored -> {});
        second.start(
            new InetSocketAddress("127.0.0.1", voters.get(1).port()),
            inbound -> inbound.close(),
            ignored -> {});
        for (int i = 0; i < 64; i++) idle.add(new Socket("127.0.0.1", voters.get(0).port()));
        second
            .send(
                0, new Frame((short) 101, false, cluster, 1, 1, id1.voterHash(), new Vote(1, 0, 0)))
            .get(2, TimeUnit.SECONDS);
        assertNotNull(frames.poll(2, TimeUnit.SECONDS), "Peer blocked by idle connections");
        second.closeAsync().get(5, TimeUnit.SECONDS);
        second = new NettyQuorumTransport(ControllerConfig.defaults(id1), clock);
        second.start(
            new InetSocketAddress("127.0.0.1", voters.get(1).port()),
            inbound -> inbound.close(),
            ignored -> {});
        for (int i = 0; i < 8; i++) idle.add(new Socket("127.0.0.1", voters.get(0).port()));
        second
            .send(
                0, new Frame((short) 101, false, cluster, 1, 2, id1.voterHash(), new Vote(2, 0, 0)))
            .get(2, TimeUnit.SECONDS);
        assertEquals(
            2,
            Objects.requireNonNull(
                    frames.poll(2, TimeUnit.SECONDS), "Peer reconnect blocked by idle connections")
                .requestId());
      } finally {
        first.closeAsync().get(5, TimeUnit.SECONDS);
        second.closeAsync().get(5, TimeUnit.SECONDS);
      }
    } finally {
      for (var socket : idle) socket.close();
    }
  }
}
