package vn.huyqt.logbroker.broker.cluster;

import static java.nio.file.StandardOpenOption.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.storage.LogConfig;

/** Owns the broker root lock until all storage workers have drained and closed their files. */
public final class BrokerIdentityStore implements AutoCloseable {
    public record Identity(UUID clusterId,int brokerId,UUID storageId) {
        public Identity {
            Objects.requireNonNull(clusterId); Objects.requireNonNull(storageId);
            if (clusterId.equals(new UUID(0,0)) || storageId.equals(new UUID(0,0)) || brokerId<0)
                throw new IllegalArgumentException("Invalid broker identity");
        }
    }
    private final Identity identity;
    private final FileChannel channel;
    private final FileLock lock;
    private BrokerIdentityStore(Identity identity,FileChannel channel,FileLock lock) {
        this.identity=identity; this.channel=channel; this.lock=lock;
    }
    /** Formats only an empty root. Identity is published last after every initialized component is durable. */
    public static Identity format(Path root,UUID clusterId,int brokerId,DurableFiles files) throws IOException {
        var identity=new Identity(clusterId,brokerId,UUID.randomUUID());
        if (Files.exists(root)) try (var children=Files.list(root)) {
            if (children.findAny().isPresent()) throw new IOException("Format requires an empty broker root");
        }
        Files.createDirectories(root); files.verifySupport(root);
        files.writeNew(root.resolve(".broker.lock"),new byte[0]);
        Files.createDirectory(root.resolve("partitions")); files.syncDirectory(root.resolve("partitions"));
        ObserverStore.format(root.resolve("observer"),clusterId,files,LogConfig.defaults());
        // INIT framing is shared with the inventory reader/provisioning implementation (Task 11).
        byte[] init=new byte[39]; var out=ByteBuffer.wrap(init);
        out.putInt(0x50494e32).putShort((short)2).putInt(init.length).putLong(1).put((byte)0);
        out.putLong(identity.storageId().getMostSignificantBits()).putLong(identity.storageId().getLeastSignificantBits());
        out.putInt(StateJournal.crc(init,0,init.length-4)); files.writeNew(root.resolve("partition-inventory.journal"),init);
        files.syncDirectory(root);
        files.writeNew(root.resolve("broker-identity.bin"),encode(identity)); files.syncDirectory(root);
        if (root.toAbsolutePath().getParent()!=null) files.syncDirectory(root.toAbsolutePath().getParent());
        return identity;
    }
    /** Opens existing formatted identity only; never creates missing roots or journal components. */
    public static BrokerIdentityStore open(Path root,UUID clusterId,int brokerId,DurableFiles files) throws IOException {
        Path path=root.resolve("broker-identity.bin");
        if (!Files.isRegularFile(path) || Files.size(path)!=50 || !Files.isRegularFile(root.resolve(".broker.lock"))
            || !Files.isRegularFile(root.resolve("partition-inventory.journal"))
            || !Files.isRegularFile(root.resolve("observer/observer-state.journal"))
            || !Files.isRegularFile(root.resolve("observer/observer-identity.bin")) || !Files.isDirectory(root.resolve("partitions")))
            throw new IOException("Broker root is missing or incompletely formatted");
        byte[] bytes=Files.readAllBytes(path); Identity identity=decode(bytes);
        if (!identity.clusterId().equals(clusterId) || identity.brokerId()!=brokerId) throw new IOException("Broker identity mismatch");
        files.verifySupport(root); FileChannel channel=FileChannel.open(root.resolve(".broker.lock"),READ,WRITE);
        try {
            FileLock lock=channel.tryLock(); if (lock==null) throw new IOException("Broker root is locked");
            return new BrokerIdentityStore(identity,channel,lock);
        } catch (IOException | RuntimeException e) {
            channel.close(); if (e instanceof OverlappingFileLockException) throw new IOException("Broker root is locked",e); throw e;
        }
    }
    private static byte[] encode(Identity identity) {
        byte[] bytes=new byte[50]; var out=ByteBuffer.wrap(bytes);
        out.putInt(0x42494432).putShort((short)2).putInt(bytes.length).putLong(identity.clusterId().getMostSignificantBits())
            .putLong(identity.clusterId().getLeastSignificantBits()).putInt(identity.brokerId())
            .putLong(identity.storageId().getMostSignificantBits()).putLong(identity.storageId().getLeastSignificantBits());
        out.putInt(StateJournal.crc(bytes,0,bytes.length-4)); return bytes;
    }
    private static Identity decode(byte[] bytes) throws IOException {
        var in=ByteBuffer.wrap(bytes);
        if (bytes.length!=50 || in.getInt()!=0x42494432 || in.getShort()!=2 || in.getInt()!=50
            || in.getInt(46)!=StateJournal.crc(bytes,0,46)) throw new IOException("Invalid broker identity format or checksum");
        try { return new Identity(new UUID(in.getLong(),in.getLong()),in.getInt(),new UUID(in.getLong(),in.getLong())); }
        catch (IllegalArgumentException e) { throw new IOException("Invalid broker identity",e); }
    }
    public Identity identity() { return identity; }
    @Override public void close() throws IOException { try { lock.release(); } finally { channel.close(); } }
}
