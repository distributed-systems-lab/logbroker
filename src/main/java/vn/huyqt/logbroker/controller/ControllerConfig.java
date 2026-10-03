package vn.huyqt.logbroker.controller;

import java.time.Duration;
import java.util.Objects;
import vn.huyqt.logbroker.storage.LogConfig;

/**
 * Immutable controller limits, validated together so progress cannot require an oversized frame.
 *
 * <p>The compact constructor rejects any combination that violates the relations below with
 * {@link IllegalArgumentException}, so every instance is valid. Byte sizes are in bytes; {@link
 * #maxFrameBytes()} excludes the 4-byte length prefix. Defaults come from {@link #builder}; the
 * property names and the operator view of these limits are in {@code
 * docs/controller-configuration.md}.
 *
 * <ul>
 *   <li>All durations are positive, and {@code fetchIdleWait < rpcTimeout < electionMin <
 *       electionMax}.
 *   <li>{@code maxFrameBytes} is in 1024..8 MiB.
 *   <li>{@code fetchMaxBytes} is at least the log max batch size and at most {@code maxFrameBytes
 *       - 1024}; the log max batch size is at least 336.
 *   <li>{@code snapshotChunkBytes} is in 1..256 KiB and at most {@code maxFrameBytes - 1024}.
 *   <li>{@code snapshotMaxBytes} is at most 64 MiB and holds the bounded cluster image plus its envelope.
 *   <li>{@code snapshotTriggerBytes} is positive.
 *   <li>{@code maxPendingRequests} is in 1..1024.
 *   <li>{@code diskQueueCapacity} is in 32..256 and {@code eventQueueCapacity} is above {@code
 *       diskQueueCapacity + 512} and at most 4096.
 *   <li>{@code inboundBytes} and {@code outboundBytes} are at least {@code maxFrameBytes} and at
 *       most 64 MiB; {@code outboundBytes} is also at least 32 times the log max batch size.
 *   <li>{@code maxTopics} is positive and {@code maxPartitions >= maxTopics}; the image byte budget bounds both.
 * </ul>
 */
public record ControllerConfig(
    ClusterIdentity identity,
    LogConfig logConfig,
    Duration fetchIdleWait,
    Duration rpcTimeout,
    Duration electionMin,
    Duration electionMax,
    Duration leaderContactTimeout,
    Duration adminTimeout,
    Duration shutdownTimeout,
    int maxFrameBytes,
    int fetchMaxBytes,
    int snapshotChunkBytes,
    int snapshotMaxBytes,
    long snapshotTriggerBytes,
    int maxPendingRequests,
    int eventQueueCapacity,
    int diskQueueCapacity,
    long inboundBytes,
    long outboundBytes,
    int maxTopics,
    int maxPartitions) {
  public ControllerConfig {
    if (identity.metadataVersion() == 2 && logConfig.maxBatchBytes() < Math.max(372L, 74L + 79L * maxPartitions))
      throw new IllegalArgumentException("Cluster lifecycle batch cannot fit configured partitions");
    validateValues(
        identity,
        logConfig,
        fetchIdleWait,
        rpcTimeout,
        electionMin,
        electionMax,
        leaderContactTimeout,
        adminTimeout,
        shutdownTimeout,
        maxFrameBytes,
        fetchMaxBytes,
        snapshotChunkBytes,
        snapshotMaxBytes,
        snapshotTriggerBytes,
        maxPendingRequests,
        eventQueueCapacity,
        diskQueueCapacity,
        inboundBytes,
        outboundBytes,
        maxTopics,
        maxPartitions);
  }

  private static void validateValues(
      ClusterIdentity identity,
      LogConfig log,
      Duration idle,
      Duration rpc,
      Duration min,
      Duration max,
      Duration contact,
      Duration admin,
      Duration shutdown,
      int frame,
      int fetch,
      int chunk,
      int snapshot,
      long trigger,
      int pending,
      int events,
      int disks,
      long inbound,
      long outbound,
      int topics,
      int partitions) {
    Objects.requireNonNull(identity);
    Objects.requireNonNull(log);
    for (var d : new Duration[] {idle, rpc, min, max, contact, admin, shutdown})
      if (d == null || d.isZero() || d.isNegative())
        throw new IllegalArgumentException("Invalid timeout");
    if (min.compareTo(max) >= 0
        || idle.compareTo(rpc) >= 0
        || rpc.compareTo(min) >= 0
        || frame < 1024
        || frame > 8 * 1024 * 1024
        || fetch < log.maxBatchBytes()
        || fetch > frame - 1024
        || chunk < 1
        || chunk > 256 * 1024
        || chunk > frame - 1024
        // Include broker identities, topic descriptors and RF=1 assignments plus the envelope.
        || snapshot < 166L + 32L * 512 + (long) topics * 320 + (long) partitions * 128
        || snapshot > 64 * 1024 * 1024
        || log.maxBatchBytes() < 336
        || trigger <= 0
        || pending < 1
        || pending > 1024
        || disks < 32
        || disks > 256
        // ControllerNode reserves 512 event slots for control and one per disk completion; the
        // remainder, which must be non-empty, is shared by admin and snapshot events.
        || events <= disks + 512
        || events > 4096
        || inbound < frame
        || outbound < frame
        || outbound < 32L * log.maxBatchBytes()
        || inbound > 64L * 1024 * 1024
        || outbound > 64L * 1024 * 1024
        || topics < 1
        || partitions < topics
        ) throw new IllegalArgumentException("Inconsistent controller limits");
  }

  /** Does nothing; every instance was already validated by the canonical constructor. */
  public void validate() {
    /* The compact constructor establishes the invariant. */
  }

  /** Image count budgets shared by cluster apply, snapshot encoding and recovery. */
  public vn.huyqt.logbroker.controller.metadata.MetadataLimits metadataLimits() {
    return new vn.huyqt.logbroker.controller.metadata.MetadataLimits(32, maxTopics, maxPartitions,
        snapshotMaxBytes - 102);
  }

  /** Returns the documented defaults for {@code identity}; see {@link Builder}. */
  public static ControllerConfig defaults(ClusterIdentity identity) {
    return builder(identity).build();
  }

  /** Returns a builder preloaded with the defaults listed on {@link Builder}. */
  public static Builder builder(ClusterIdentity identity) {
    return new Builder(identity);
  }

  /**
   * Mutable builder whose setters do not validate; {@link #build()} applies the relations
   * documented on {@link ControllerConfig} and throws {@link IllegalArgumentException} if they do
   * not hold.
   *
   * <p>Defaults: {@link LogConfig#defaults()}; fetch idle wait 100 ms; RPC timeout 1 s; election
   * timeout 1500..3000 ms; leader contact timeout 3 s; admin and shutdown timeouts 30 s; max frame
   * 8 MiB; fetch max 4 MiB; snapshot chunk 256 KiB; snapshot max 64 MiB; snapshot trigger 16 MiB;
   * 1024 pending requests; event queue 4096; disk queue 256; inbound and outbound budgets 64 MiB
   * each; 128 topics; 1024 partitions.
   */
  public static final class Builder {
    private final ClusterIdentity identity;
    private LogConfig logConfig = LogConfig.defaults();
    private Duration fetchIdleWait = Duration.ofMillis(100),
        rpcTimeout = Duration.ofSeconds(1),
        electionMin = Duration.ofMillis(1500),
        electionMax = Duration.ofSeconds(3),
        leaderContactTimeout = Duration.ofSeconds(3),
        adminTimeout = Duration.ofSeconds(30),
        shutdownTimeout = Duration.ofSeconds(30);
    private int maxFrameBytes = 8 * 1024 * 1024,
        fetchMaxBytes = 4 * 1024 * 1024,
        snapshotChunkBytes = 256 * 1024,
        snapshotMaxBytes = 64 * 1024 * 1024,
        maxPendingRequests = 1024,
        eventQueueCapacity = 4096,
        diskQueueCapacity = 256,
        maxTopics = 128,
        maxPartitions = 1024;
    private long snapshotTriggerBytes = 16L * 1024 * 1024,
        inboundBytes = 64L * 1024 * 1024,
        outboundBytes = 64L * 1024 * 1024;

    private Builder(ClusterIdentity identity) {
      this.identity = identity;
    }

    public Builder logConfig(LogConfig value) {
      logConfig = value;
      return this;
    }

    public Builder fetchIdleWait(Duration value) {
      fetchIdleWait = value;
      return this;
    }

    public Builder rpcTimeout(Duration value) {
      rpcTimeout = value;
      return this;
    }

    public Builder electionMin(Duration value) {
      electionMin = value;
      return this;
    }

    public Builder electionMax(Duration value) {
      electionMax = value;
      return this;
    }

    public Builder leaderContactTimeout(Duration value) {
      leaderContactTimeout = value;
      return this;
    }

    public Builder adminTimeout(Duration value) {
      adminTimeout = value;
      return this;
    }

    public Builder shutdownTimeout(Duration value) {
      shutdownTimeout = value;
      return this;
    }

    public Builder maxFrameBytes(int value) {
      maxFrameBytes = value;
      return this;
    }

    public Builder fetchMaxBytes(int value) {
      fetchMaxBytes = value;
      return this;
    }

    public Builder snapshotChunkBytes(int value) {
      snapshotChunkBytes = value;
      return this;
    }

    public Builder snapshotMaxBytes(int value) {
      snapshotMaxBytes = value;
      return this;
    }

    /** Storage bytes of batches appended since the latest snapshot that trigger a new one. */
    public Builder snapshotTriggerBytes(long value) {
      snapshotTriggerBytes = value;
      return this;
    }

    /**
     * Admin invocations admitted concurrently by the node's {@link ControllerService}. Quorum
     * admission is further capped by the disk queue; see {@code docs/controller-configuration.md}.
     */
    public Builder maxPendingRequests(int value) {
      maxPendingRequests = value;
      return this;
    }

    public Builder eventQueueCapacity(int value) {
      eventQueueCapacity = value;
      return this;
    }

    public Builder diskQueueCapacity(int value) {
      diskQueueCapacity = value;
      return this;
    }

    /**
     * Node transport budget for inbound frames; one eighth, capped at 8 MiB, is reserved for peer
     * control traffic. {@link #outboundBytes} is split the same way.
     */
    public Builder inboundBytes(long value) {
      inboundBytes = value;
      return this;
    }

    public Builder outboundBytes(long value) {
      outboundBytes = value;
      return this;
    }

    public Builder maxTopics(int value) {
      maxTopics = value;
      return this;
    }

    /** Total partitions across all topics. */
    public Builder maxPartitions(int value) {
      maxPartitions = value;
      return this;
    }

    public ControllerConfig build() {
      return new ControllerConfig(
          identity,
          logConfig,
          fetchIdleWait,
          rpcTimeout,
          electionMin,
          electionMax,
          leaderContactTimeout,
          adminTimeout,
          shutdownTimeout,
          maxFrameBytes,
          fetchMaxBytes,
          snapshotChunkBytes,
          snapshotMaxBytes,
          snapshotTriggerBytes,
          maxPendingRequests,
          eventQueueCapacity,
          diskQueueCapacity,
          inboundBytes,
          outboundBytes,
          maxTopics,
          maxPartitions);
    }
  }
}
