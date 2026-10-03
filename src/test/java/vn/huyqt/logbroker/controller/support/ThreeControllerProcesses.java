package vn.huyqt.logbroker.controller.support;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.client.*;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

public final class ThreeControllerProcesses implements AutoCloseable {
  private final Path root;
  private final UUID cluster = UUID.randomUUID();
  private final Process[] children = new Process[3];
  private final int[] backend = new int[3];
  private final List<ClusterIdentity.Voter> voters = new ArrayList<>();
  private final ControllerFaultProxy[] proxies = new ControllerFaultProxy[3];
  private final Set<Integer> isolated = ConcurrentHashMap.newKeySet();
  private final DeadlineScheduler clock = DeadlineScheduler.system();
  private ControllerClient client;

  public ThreeControllerProcesses(Path root) throws Exception {
    this.root = root;
    Files.createDirectories(root);
    new DurableFiles().verifySupport(root);
    try {
      for (int i = 0; i < 3; i++) {
        try (var socket = new ServerSocket(0)) {
          backend[i] = socket.getLocalPort();
        }
        proxies[i] = new ControllerFaultProxy(i, backend[i], isolated);
        voters.add(new ClusterIdentity.Voter(i, "127.0.0.1", proxies[i].port()));
      }
      var identity = new ClusterIdentity(cluster, 0, voters);
      client =
          new ControllerClient(
              identity,
              voters.stream().map(v -> new InetSocketAddress(v.host(), v.port())).toList(),
              clock,
              new NettyControllerClientTransport.Factory(identity, clock));
      String membership =
          voters.stream()
              .map(v -> v.id() + "@" + v.host() + ":" + v.port())
              .collect(java.util.stream.Collectors.joining(","));
      for (int i = 0; i < 3; i++) {
        QuorumStateStore.format(
            data(i), new ClusterIdentity(cluster, i, voters), new DurableFiles());
        Files.writeString(
            config(i),
            "cluster.id="
                + cluster
                + "\nnode.id="
                + i
                + "\nvoters="
                + membership
                + "\ndata.dir="
                + data(i)
                + "\nfetch.idle.wait.ms=20\nrpc.timeout.ms=100\nelection.min.ms=300\nelection.max.ms=600\nleader.contact.timeout.ms=1000\nsnapshot.trigger.bytes=100\nlog.segment.bytes=512\nlog.max.batch.bytes=336\nlog.index.interval.bytes=64\n");
      }
    } catch (Exception error) {
      close();
      throw error;
    }
  }

  public Path data(int node) {
    return root.resolve("node-" + node);
  }

  private Path config(int node) {
    return root.resolve("node-" + node + ".properties");
  }

  public ControllerClient client() {
    return client;
  }

  public void startAll() throws Exception {
    for (int i = 0; i < 3; i++) restart(i);
  }

  public void restart(int node) throws Exception {
    if (children[node] != null && children[node].isAlive())
      throw new IllegalStateException("Already alive");
    children[node] =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                ControllerProcessMain.class.getName(),
                config(node).toString(),
                Integer.toString(backend[node]))
            .redirectOutput(
                ProcessBuilder.Redirect.appendTo(root.resolve("stdout-" + node + ".log").toFile()))
            .redirectError(
                ProcessBuilder.Redirect.appendTo(root.resolve("stderr-" + node + ".log").toFile()))
            .start();
  }

  public void kill(int node) throws Exception {
    var process = children[node];
    if (process != null) {
      process.destroyForcibly();
      if (!process.waitFor(10, TimeUnit.SECONDS))
        throw new AssertionError("Owned child did not terminate");
      children[node] = null;
    }
  }

  public void isolate(int node) {
    isolated.add(node);
    for (var proxy : proxies) proxy.refreshIsolation();
  }

  public void heal() {
    isolated.clear();
  }

  public int awaitLeader(Duration timeout) throws Exception {
    long end = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < end) {
      for (int i = 0; i < 3; i++)
        if (children[i] != null && children[i].isAlive())
          try {
            var status = client.describe(i).get(2, TimeUnit.SECONDS);
            if (status.ready()
                && status.role() == QuorumStatus.Role.LEADER
                && !isolated.contains(i)) return i;
          } catch (ExecutionException | TimeoutException ignored) {
          }
      Thread.sleep(20);
    }
    throw failure("No ready leader");
  }

  public void awaitConvergence(Duration timeout) throws Exception {
    var expected = client.metadata().get(30, TimeUnit.SECONDS);
    long end = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < end) {
      boolean same = true;
      for (int i = 0; i < 3; i++)
        try {
          var local = client.localMetadata(i).get(2, TimeUnit.SECONDS);
          same &= local.topics().equals(expected.topics()) && local.applied() >= expected.applied();
        } catch (ExecutionException | TimeoutException error) {
          same = false;
        }
      if (same) return;
      Thread.sleep(20);
    }
    throw failure("Catalogs did not converge");
  }

  public void awaitPrefixDeletion(Duration timeout) throws Exception {
    long end = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < end) {
      for (int i = 0; i < 3; i++)
        try (var walk = Files.walk(data(i).resolve("generations"))) {
          var segments = walk.filter(p -> p.getFileName().toString().endsWith(".log")).toList();
          if (!segments.isEmpty()
              && segments.stream()
                  .noneMatch(p -> p.getFileName().toString().startsWith("00000000000000000000")))
            return;
        }
      Thread.sleep(20);
    }
    throw failure("No real segment prefix deletion");
  }

  public void assertMinorityBarrierFails(int node, Duration timeout) throws Exception {
    var identity = new ClusterIdentity(cluster, 0, voters);
    var factory = new NettyControllerClientTransport.Factory(identity, clock);
    var wire = factory.create();
    var response = new CompletableFuture<Frame>();
    try {
      wire.connect(
              new InetSocketAddress("127.0.0.1", voters.get(node).port()),
              response::complete,
              response::completeExceptionally)
          .get(2, TimeUnit.SECONDS);
      wire.send(
              new Frame(
                  (short) 108,
                  false,
                  cluster,
                  -1,
                  99999,
                  identity.voterHash(),
                  new ReadMetadata(700)))
          .get(2, TimeUnit.SECONDS);
      var reply = (Reply) response.get(timeout.toMillis(), TimeUnit.MILLISECONDS).message();
      if (reply.meta().error() == QuorumError.NONE)
        throw new AssertionError("Isolated minority served linearizable read");
    } finally {
      wire.close();
      factory.close();
    }
  }

  private AssertionError failure(String text) {
    return new AssertionError(text + "; process logs: " + root);
  }

  public void close() throws Exception {
    if (client != null) client.close();
    clock.close();
    for (int i = 0; i < 3; i++) kill(i);
    for (var proxy : proxies) if (proxy != null) proxy.close();
    Path retained =
        Path.of("target", "controller-process-logs", root.getFileName() + "-" + cluster);
    Files.createDirectories(retained);
    for (int i = 0; i < 3; i++)
      for (String kind : List.of("stdout", "stderr")) {
        Path log = root.resolve(kind + "-" + i + ".log");
        if (Files.exists(log))
          Files.copy(log, retained.resolve(log.getFileName()), StandardCopyOption.REPLACE_EXISTING);
      }
  }
}
