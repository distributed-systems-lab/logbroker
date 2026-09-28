package vn.huyqt.logbroker.controller;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Fixed membership and node identity; the hash deliberately excludes local node ID. */
public record ClusterIdentity(UUID clusterId, int nodeId, List<Voter> voters) {
    public record Voter(int id, String host, int port) {
        public Voter {
            Objects.requireNonNull(host);
            if (id < 0 || host.isBlank() || host.getBytes(StandardCharsets.UTF_8).length > 255
                    || port < 1 || port > 65535) throw new IllegalArgumentException("Invalid voter endpoint");
        }
    }
    public ClusterIdentity {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(voters);
        voters = voters.stream().sorted(Comparator.comparingInt(Voter::id)).toList();
        if (clusterId.equals(new UUID(0, 0)) || voters.size() != 3 || nodeId < 0
                || voters.stream().noneMatch(v -> v.id() == nodeId)
                || voters.stream().map(Voter::id).distinct().count() != 3
                || voters.stream().map(v -> v.host() + ":" + v.port()).distinct().count() != 3)
            throw new IllegalArgumentException("Expected three distinct voters including this node");
    }
    public byte[] canonicalVoters() {
        try {
            var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes);
            out.writeInt(voters.size());
            for (var voter : voters) {
                byte[] host = voter.host().getBytes(StandardCharsets.UTF_8);
                out.writeInt(voter.id()); out.writeInt(host.length); out.write(host); out.writeInt(voter.port());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) { throw new UncheckedIOException(impossible); }
    }
    public byte[] voterHash() {
        try { return MessageDigest.getInstance("SHA-256").digest(canonicalVoters()); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    public Voter voter(int id) {
        return voters.stream().filter(v -> v.id() == id).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown voter " + id));
    }
}
