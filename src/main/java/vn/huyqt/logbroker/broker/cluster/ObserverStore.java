package vn.huyqt.logbroker.broker.cluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.function.BooleanSupplier;
import vn.huyqt.logbroker.controller.log.*;
import vn.huyqt.logbroker.controller.metadata.*;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.snapshot.*;
import vn.huyqt.logbroker.storage.LogConfig;

/** Single ordered-worker owner of a committed observer cache; never persists votes or voter epochs. */
public final class ObserverStore implements AutoCloseable {
    public static final class InstallCancelled extends IOException {
        public InstallCancelled() { super("Stale observer install"); }
    }
    private final Path root;
    private final UUID clusterId;
    private final DurableFiles files;
    private final LogConfig config;
    private final MetadataLimits limits;
    private final StateJournal journal;
    private final MetadataStateMachine metadata;
    private volatile UUID generation;
    private long committed,start;
    private volatile long prefixEpoch;
    private SnapshotId base;
    private QuorumLog log;
    private SnapshotStore snapshots;
    private byte[] membership;
    private boolean failed;
    private ObserverStore(Path root,UUID clusterId,DurableFiles files,LogConfig config,MetadataLimits limits,StateJournal journal) {
        this.root=root; this.clusterId=clusterId; this.files=files; this.config=config; this.limits=limits; this.journal=journal;
        metadata=new MetadataStateMachine(limits,(short)2);
    }
    /** Initializes a fresh observer root; root identity is published after its generation is durable. */
    public static void format(Path root,UUID cluster,DurableFiles files,LogConfig config) throws IOException {
        if (cluster==null || cluster.equals(new UUID(0,0))) throw new IllegalArgumentException("Invalid observer cluster");
        if (Files.exists(root)) try (var children=Files.list(root)) {
            if (children.findAny().isPresent()) throw new IOException("Observer format requires empty root");
        }
        Files.createDirectories(root); files.verifySupport(root); UUID generation=UUID.randomUUID();
        var logPath=root.resolve("generations").resolve(generation.toString()).resolve("log"); Files.createDirectories(logPath);
        try (var log=QuorumLog.open(logPath,config,0,0,true,files)) { log.flush(); }
        files.syncDirectory(logPath); files.syncDirectory(logPath.getParent()); files.syncDirectory(root.resolve("generations"));
        Files.createDirectory(root.resolve("snapshots")); files.syncDirectory(root.resolve("snapshots")); files.syncDirectory(root);
        try (var journal=StateJournal.open(root.resolve("observer-state.journal"),files)) {
            journal.append(StateJournal.GENERATION,GenerationStore.encodeGeneration(generation,0,null));
            journal.append(StateJournal.COMMIT,ByteBuffer.allocate(8).putLong(0).array());
        }
        byte[] identity=new byte[30]; ByteBuffer.wrap(identity).putInt(0x4f494432).putShort((short)2).putInt(30)
            .putLong(cluster.getMostSignificantBits()).putLong(cluster.getLeastSignificantBits())
            .putInt(StateJournal.crc(identity,0,26));
        files.writeNew(root.resolve("observer-identity.bin"),identity); files.syncDirectory(root);
        files.syncDirectory(root.toAbsolutePath().getParent());
    }
    public static ObserverStore open(Path root,UUID cluster,DurableFiles files,LogConfig config,MetadataLimits limits) throws IOException {
        Path identity=root.resolve("observer-identity.bin");
        if (!Files.isRegularFile(identity) || Files.size(identity)!=30 || !Files.isRegularFile(root.resolve("observer-state.journal")))
            throw new IOException("Observer root is missing or unformatted");
        byte[] bytes=Files.readAllBytes(identity); var in=ByteBuffer.wrap(bytes);
        if (in.getInt()!=0x4f494432 || in.getShort()!=2 || in.getInt()!=30
            || !new UUID(in.getLong(),in.getLong()).equals(cluster) || in.getInt()!=StateJournal.crc(bytes,0,26))
            throw new IOException("Observer identity mismatch or corruption");
        files.verifySupport(root); var journal=StateJournal.open(root.resolve("observer-state.journal"),files);
        var store=new ObserverStore(root,cluster,files,config,limits,journal);
        try { store.recover(); return store; }
        catch (IOException | RuntimeException e) { try { store.close(); } catch (IOException close) { e.addSuppressed(close); } throw e; }
    }
    private void recover() throws IOException {
        for (var frame:journal.frames()) {
            var in=ByteBuffer.wrap(frame.payload());
            try {
                if (frame.type()==StateJournal.GENERATION) {
                    UUID next=new UUID(in.getLong(),in.getLong()); long offset=in.getLong(); byte present=in.get();
                    if (offset<0 || present!=0 && present!=1 || next.equals(new UUID(0,0))) throw new IOException("Invalid observer generation");
                    SnapshotId snapshot=present==1 ? SnapshotId.readFrom(in) : null;
                    if (snapshot!=null && snapshot.endOffset()!=offset || in.hasRemaining()) throw new IOException("Invalid observer base");
                    generation=next; start=offset; base=snapshot; committed=offset;
                } else if (frame.type()==StateJournal.COMMIT) {
                    long end=in.getLong(); if (generation==null || end<committed || in.hasRemaining()) throw new IOException("Invalid observer checkpoint");
                    committed=end;
                } else if (frame.type()!=StateJournal.SNAPSHOT_SET) throw new IOException("Unsupported observer journal record");
            } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) { throw new IOException("Invalid observer journal",e); }
        }
        if (generation==null) throw new IOException("Missing observer generation");
        Path membershipPath=root.resolve("membership.bin");
        if (Files.exists(membershipPath)) {
            if (Files.size(membershipPath)!=36) throw new IOException("Invalid membership length");
            byte[] bytes=Files.readAllBytes(membershipPath);
            if (ByteBuffer.wrap(bytes).getInt(32)!=StateJournal.crc(bytes,0,32)) throw new IOException("Invalid membership checksum");
            membership=Arrays.copyOf(bytes,32);
            if (Arrays.equals(membership,new byte[32])) throw new IOException("Invalid zero membership");
        }
        snapshots=new SnapshotStore(root,clusterId,membership==null ? new byte[32] : membership,files,SnapshotJournal.of(journal),64*1024*1024,limits);
        if (base!=null) {
            if (membership==null) throw new IOException("Snapshot without verified membership");
            metadata.restore(snapshots.load(base));
        }
        log=QuorumLog.open(directory(),config,start,committed,base==null ? 0 : base.lastEpoch(),false,files);
        log.epochs().positionAt(committed);
        // A tail without a forced coverage checkpoint was never applied or acknowledged.
        if (log.end()>committed) log.truncate(committed,committed);
        while (metadata.image().appliedOffset()<committed) {
            var batches=log.read(metadata.image().appliedOffset(),config.maxBatchBytes());
            if (batches.isEmpty()) throw new IOException("Missing committed observer data");
            for (var batch:batches) if (batch.nextOffset()<=committed) metadata.apply(batch);
        }
        prefixEpoch=log.epochs().positionAt(committed).lastEpoch();
    }
    private Path directory() { return root.resolve("generations").resolve(generation.toString()).resolve("log"); }
    /** Pins the independently verified fixed-voter hash before accepting any controller snapshot. */
    public void pinMembership(byte[] hash) throws IOException {
        healthy(); if (hash.length!=32 || Arrays.equals(hash,new byte[32])) throw new IOException("Invalid membership hash");
        if (membership!=null) { if (!Arrays.equals(membership,hash)) throw new IOException("Observer membership mismatch"); return; }
        byte[] bytes=Arrays.copyOf(hash,36); ByteBuffer.wrap(bytes).putInt(32,StateJournal.crc(bytes,0,32));
        try {
            files.writeNew(root.resolve("membership.bin"),bytes); files.syncDirectory(root); membership=hash.clone();
            snapshots.close(); snapshots=new SnapshotStore(root,clusterId,membership,files,SnapshotJournal.of(journal),64*1024*1024,limits);
        } catch (IOException e) { failed=true; throw e; }
    }
    /** Validates the entire received prefix before writing; checkpoints only its actual contiguous end. */
    public void appendCommitted(List<QuorumBatch> batches,long verifiedCommit) throws IOException {
        healthy(); if (verifiedCommit<committed) throw new IOException("Observer commit coverage regression");
        long end=committed; var next=new MetadataStateMachine(limits,(short)2); next.restore(metadata.image());
        for (var batch:batches) {
            if (batch.baseOffset()!=end || batch.nextOffset()>verifiedCommit) throw new IOException("Uncommitted or noncontiguous observer batch");
            next.apply(batch); end=batch.nextOffset();
        }
        if (batches.isEmpty()) return;
        try {
            for (var batch:batches) log.appendReplica(batch); log.flush();
            journal.append(StateJournal.COMMIT,ByteBuffer.allocate(8).putLong(end).array()); committed=end;
            prefixEpoch=log.epochs().positionAt(end).lastEpoch(); metadata.restore(next.image());
        } catch (IOException | RuntimeException e) { failed=true; throw e; }
    }
    /** Creates an empty replacement log; the forced GENERATION frame alone publishes it. */
    public void install(SnapshotId id,MetadataImage image,BooleanSupplier allowed) throws IOException {
        healthy(); if (membership==null) throw new IOException("Snapshot membership not verified");
        if (!allowed.getAsBoolean()) throw new InstallCancelled();
        if (image.appliedOffset()!=id.endOffset() || id.endOffset()<committed || id.lastEpoch()<log.epochs().positionAt(committed).lastEpoch())
            throw new IOException("Snapshot contradicts committed observer prefix");
        image.validate(limits);
        var validator=new MetadataStateMachine(limits,(short)2); validator.restore(image);
        if (!Files.isRegularFile(snapshots.path(id))) {
            Path partial=root.resolve("snapshots").resolve(id.contentId()+".install.partial");
            files.writeNew(partial,snapshots.encode(id,image)); Files.move(partial,snapshots.path(id),StandardCopyOption.ATOMIC_MOVE);
            files.syncDirectory(root.resolve("snapshots"));
        }
        if (!image.equals(snapshots.load(id))) throw new IOException("Snapshot image mismatch");
        UUID next=UUID.randomUUID(); Path directory=root.resolve("generations").resolve(next.toString()).resolve("log");
        QuorumLog replacement=QuorumLog.open(directory,config,id.endOffset(),id.endOffset(),id.lastEpoch(),true,files);
        boolean published=false;
        try {
            replacement.flush(); files.syncDirectory(directory); files.syncDirectory(directory.getParent()); files.syncDirectory(directory.getParent().getParent());
            if (!allowed.getAsBoolean()) throw new InstallCancelled();
            journal.append(StateJournal.GENERATION,GenerationStore.encodeGeneration(next,id.endOffset(),id)); published=true;
            var old=log; log=replacement; generation=next; start=committed=id.endOffset(); base=id;
            prefixEpoch=id.lastEpoch(); metadata.restore(image); old.close();
            snapshots.publishInstalled(id);
        } catch (IOException | RuntimeException e) { if (!(e instanceof InstallCancelled)) failed=true; throw e; }
        finally { if (!published) replacement.close(); }
    }
    private void healthy() throws IOException { if (failed) throw new IOException("Observer store failed; restart required"); }
    public MetadataImage image() { return metadata.image(); }
    public UUID generation() { return generation; }
    public long durableEnd() { return committed; }
    /** Cached at durable publication, so the observer loop never reads a disk-owned epoch index. */
    public long prefixEpoch() { return prefixEpoch; }
    public SnapshotStore snapshots() throws IOException { healthy(); if (membership==null) throw new IOException("Membership discovery required"); return snapshots; }

    /** Deletes only unreferenced generation directories after publication and old-log close. */
    public boolean releaseObsoleteGenerations() {
        Path parent=root.resolve("generations").toAbsolutePath().normalize();
        try (var children=Files.list(parent)) {
            for (var child:children.toList()) {
                Path target=child.toAbsolutePath().normalize(); UUID id;
                try { id=UUID.fromString(target.getFileName().toString()); } catch (IllegalArgumentException ignored) { continue; }
                if (id.equals(generation) || !id.toString().equals(target.getFileName().toString())
                    || !target.getParent().equals(parent) || !Files.isDirectory(target,LinkOption.NOFOLLOW_LINKS)) continue;
                try (var paths=Files.walk(target)) {
                    for (var path:paths.sorted(Comparator.reverseOrder()).toList()) {
                        if (!path.toAbsolutePath().normalize().startsWith(target)) throw new IOException("Observer cleanup escaped root");
                        Files.delete(path);
                    }
                }
            }
            files.syncDirectory(parent); snapshots.releaseObsolete(); return true;
        } catch (IOException e) { return false; }
    }
    @Override public void close() throws IOException {
        IOException failure=null;
        try { if (snapshots!=null) snapshots.close(); } catch (IOException e) { failure=e; }
        try { if (log!=null) log.close(); } catch (IOException e) { if (failure==null) failure=e; else failure.addSuppressed(e); }
        try { journal.close(); } catch (IOException e) { if (failure==null) failure=e; else failure.addSuppressed(e); }
        if (failure!=null) throw failure;
    }
}
