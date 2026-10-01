# Controller storage v1

Controller identity and voter set are fixed. Format explicitly; opening missing storage fails. Each root holds an exclusive `.lock` until all I/O stops. Votes are journaled and forced before publication, independent of metadata log generations.

`identity.bin`: big endian magic `0x51494431`, version i16=1, total length i32, cluster UUID (two i64), node ID i32, canonical voter list (count i32; sorted ID i32, UTF-8 host length i32 + host bytes, port i32), CRC32C of all preceding bytes.

`quorum-state.journal`: frames magic `0x514A4E31`, version i16=1, total length i32, sequence i64 starting at 1, type i16, payload and CRC32C. Minimum length 24, maximum 64 KiB; journal capacity 64 MiB. Exhaustion fails explicitly; v1 does not compact this journal. Complete checksum failures stop recovery. Only a structurally valid incomplete final frame is truncated.

Types: HARD_STATE=1 (epoch i64, votedFor i32), GENERATION=2 (UUID, start i64, snapshot-present u8 + optional snapshot ID), COMMIT=3, TRUNCATE_INTENT=4, TRUNCATE_DONE=5, SNAPSHOT_SET=6, PREFIX_INTENT=7, PREFIX_DONE=8. Unknown types/versions fail. One vote per epoch survives restart; an assigned vote cannot be cleared within that epoch.

Publication requires forcing file contents and directory entries before the journal refers to them. `DurableFiles` opens directories through the filesystem provider and forces them; failure is explicit. It does not fall back to file-only force or rename-only durability. Real provider support is verified separately from injected `FaultFiles` tests. Process crashes and simulated power loss are different test categories.

Initial probe on native Windows Java 21 returned `Directory durability unsupported`. The user selected Linux/WSL for strict controller execution. Data roots for strict acceptance must be on verified Linux filesystem storage, not assumed safe merely because the path is visible through `/mnt/d`. Core and injected-filesystem tests can run on Windows; they do not establish native Windows publication durability.

Metadata logs encode one `QuorumEntry` per ordinary storage record (timestamp=0, null key, no headers), preserving leader batch boundaries and monotonically ordered epochs. Durable truncate intent is replayed before normal recovery: only the checksummed retained prefix is scanned, then the intended suffix is removed and directory changes forced. This permits resuming an intentional truncation without disguising corruption within the retained committed prefix. Commit checkpoints are exclusive batch boundaries and cannot exceed durable coverage.

After two snapshots are published, the older retained snapshot becomes the recovery base through a forced GENERATION record before PREFIX_INTENT removes sealed segments. GENERATION start and PREFIX_INTENT start are physical segment bases; the logical quorum origin is the referenced snapshot end/epoch. A segment containing that boundary remains on disk. Recovery restores the immutable image and applies only the committed suffix after its boundary. Snapshot GC preserves the latest two IDs, the active generation's snapshot reference, and all open pins.

The v1 metadata image contains at most 128 bounded topic descriptors, so encoding fits in one 256 KiB disk slice. Snapshot creation runs at lower priority than ordered log/vote work, with one captured image in flight. Mutations retain FIFO ordering and snapshot work receives a turn after eight ordinary queued tasks.

Downloads use one bounded `.download.partial` file. Position and exact duplicate bytes are checked before writing; full identity and checksum validation precede forcing and publishing the immutable snapshot. The v1 descriptor count bounds a canonical file to at most `512 + 128 * 281` bytes, even when the configured envelope limit is larger. Uploads keep at most two file pins and expire inactive sessions after 30 seconds.

Install creates and forces a fresh empty log at snapshot end, closes the drained old log, rechecks the transfer fence, then forces GENERATION. An expired fence after closing the old log reopens it before returning cancellation. Epoch/vote remain global. A published install updates physical generation/image facts even when its initiating epoch has since ended; no old-epoch progress reply follows. Apply events carry their generation, and old-generation disk operations are discarded. Cleanup of obsolete UUID generation directories happens after publication and drain; cleanup failure leaves harmless garbage for a later retry.
