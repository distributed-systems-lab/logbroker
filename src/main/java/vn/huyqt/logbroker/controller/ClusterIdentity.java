package vn.huyqt.logbroker.controller;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/**
 * Fixed membership and node identity; the hash deliberately excludes local node ID.
 *
 * <p>Phase 3 membership is exactly three voters with distinct IDs and distinct endpoints, and
 * {@link #nodeId()} must be one of them. Voters are stored sorted by ID, so the canonical encoding
 * and {@link #voterHash()} do not depend on the order in which the voter list was written. All
 * three nodes therefore share one hash, which every frame carries (see {@code
 * docs/controller-protocol-v1.md}). The identity is a configuration check, not authentication.
 * Construction throws {@link IllegalArgumentException} for the zero cluster UUID or any other
 * membership shape.
 */
public record ClusterIdentity(UUID clusterId, int nodeId, List<Voter> voters, short metadataVersion) {
  public ClusterIdentity(UUID clusterId, int nodeId, List<Voter> voters) {
    this(clusterId, nodeId, voters, (short) 1);
  }
  /**
   * One voter endpoint: a non-negative ID, a non-blank host of at most 255 UTF-8 bytes and a port
   * in 1..65535.
   */
  public record Voter(int id, String host, int port) {
    public Voter {
      Objects.requireNonNull(host);
      if (id < 0
          || host.isBlank()
          || host.getBytes(StandardCharsets.UTF_8).length > 255
          || port < 1
          || port > 65535) throw new IllegalArgumentException("Invalid voter endpoint");
    }
  }

  public ClusterIdentity {
    if (metadataVersion != 1 && metadataVersion != 2) throw new IllegalArgumentException("Unsupported metadata version");
    Objects.requireNonNull(clusterId);
    Objects.requireNonNull(voters);
    voters = voters.stream().sorted(Comparator.comparingInt(Voter::id)).toList();
    if (clusterId.equals(new UUID(0, 0))
        || voters.size() != 3
        || nodeId < 0
        || voters.stream().noneMatch(v -> v.id() == nodeId)
        || voters.stream().map(Voter::id).distinct().count() != 3
        || voters.stream().map(v -> v.host() + ":" + v.port()).distinct().count() != 3)
      throw new IllegalArgumentException("Expected three distinct voters including this node");
  }

  /**
   * Returns the canonical membership encoding hashed by {@link #voterHash()}: voter count i32, then
   * per voter in ascending ID order its ID i32, host byte length i32, UTF-8 host and port i32, all
   * big-endian. The local node ID is not included.
   */
  public byte[] canonicalVoters() {
    return canonicalVoters(voters);
  }

  /** Canonical membership encoding without requiring a local election identity. */
  public static byte[] canonicalVoters(List<Voter> membership) {
    var voters = membership.stream().sorted(Comparator.comparingInt(Voter::id)).toList();
    if (voters.size() != 3 || voters.stream().map(Voter::id).distinct().count() != 3
        || voters.stream().map(v -> v.host() + ":" + v.port()).distinct().count() != 3)
      throw new IllegalArgumentException("Expected three distinct voters");
    try {
      var bytes = new ByteArrayOutputStream();
      var out = new DataOutputStream(bytes);
      out.writeInt(voters.size());
      for (var voter : voters) {
        byte[] host = voter.host().getBytes(StandardCharsets.UTF_8);
        out.writeInt(voter.id());
        out.writeInt(host.length);
        out.write(host);
        out.writeInt(voter.port());
      }
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new UncheckedIOException(impossible);
    }
  }

  /** Returns the SHA-256 of {@link #canonicalVoters()}; a new 32-byte array on every call. */
  public byte[] voterHash() {
    return voterHash(voters);
  }

  public static byte[] voterHash(List<Voter> voters) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(canonicalVoters(voters));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  /**
   * Returns the voter with {@code id}.
   *
   * @throws IllegalArgumentException if {@code id} is not a member
   */
  public Voter voter(int id) {
    return voters.stream()
        .filter(v -> v.id() == id)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown voter " + id));
  }
}
