package vn.huyqt.logbroker.controller.consensus;
import java.util.*;
import vn.huyqt.logbroker.controller.log.EpochIndex;
/** Remote durable matches, scoped to one leadership epoch. Local durability is supplied separately. */
public final class ReplicationTracker {
    private long epoch;
    private final Map<Integer,Long> matches=new HashMap<>();
    public void reset(long epoch){this.epoch=epoch;matches.clear();}
    public void confirm(int voter,long end,long lastEpoch,EpochIndex index) {
        try {if(index.positionAt(end).lastEpoch()==lastEpoch)matches.merge(voter,end,Math::max);}
        catch(IllegalArgumentException ignored){/* An offset alone is not matching-prefix evidence. */}
    }
    public long committable(long localDurable,long epoch,EpochIndex index) {
        if(this.epoch!=epoch)return 0;
        long remote=matches.values().stream().mapToLong(Long::longValue).max().orElse(0);
        long ceiling=Math.min(localDurable,remote);
        return index.boundaries().stream().filter(p->p.endOffset()<=ceiling&&p.lastEpoch()==epoch)
            .mapToLong(EpochIndex.LogPosition::endOffset).max().orElse(0);
    }
    public Map<Integer,Long> matches(){return Map.copyOf(matches);}
}
