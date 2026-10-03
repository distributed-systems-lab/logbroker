package vn.huyqt.logbroker.controller.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import vn.huyqt.logbroker.controller.persistence.StateJournal;

/** Borrowed journal publication contract, independent of voter identity and voting hard state. */
public interface SnapshotJournal {
    List<StateJournal.Frame> frames();
    long append(short type,byte[] payload) throws IOException;
    static SnapshotJournal of(StateJournal journal) {
        return new SnapshotJournal() {
            public List<StateJournal.Frame> frames() { return journal.frames(); }
            public long append(short type,byte[] payload) throws IOException { return journal.append(type,payload); }
        };
    }
    /** The active generation's immutable recovery base remains referenced even outside the retained set. */
    default SnapshotId snapshotReference() throws IOException {
        SnapshotId result=null;
        for (var frame:frames()) if (frame.type()==StateJournal.GENERATION) {
            try {
                var in=ByteBuffer.wrap(frame.payload()); in.getLong(); in.getLong(); in.getLong(); byte present=in.get();
                if (present!=0 && present!=1) throw new IOException("Invalid generation snapshot flag");
                result=present==1 ? SnapshotId.readFrom(in) : null;
                if (in.hasRemaining()) throw new IOException("Invalid generation snapshot reference");
            } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
                throw new IOException("Invalid generation snapshot reference",e);
            }
        }
        return result;
    }
}
