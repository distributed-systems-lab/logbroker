package vn.huyqt.logbroker.controller.snapshot;
import java.util.*;
import java.util.function.*;
import vn.huyqt.logbroker.controller.consensus.*;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
/** One loop-owned download. RPC correlation and disk completion gate every next step. */
public final class SnapshotTransfer {
    private final int chunkBytes,maxBytes;private final IntSupplier leader;
    private final Function<Request,Frame> request;private final Supplier<DiskToken> tokens;private final Consumer<QuorumEffect> emit;
    private final Map<DiskToken,Consumer<DiskResult>> pending=new HashMap<>();
    private SnapshotId id;private long epoch,position,total=-1,now,flightDeadline;private Frame flight;
    private int source;private boolean writing;private FetchSnapshotReply lastChunk;
    private volatile DiskToken installToken;
    private long rpcNanos=1_000_000_000L;
    public SnapshotTransfer(int chunkBytes,int maxBytes,IntSupplier leader,Function<Request,Frame> request,Supplier<DiskToken> tokens,Consumer<QuorumEffect> emit) {
        this.chunkBytes=chunkBytes;this.maxBytes=maxBytes;this.leader=leader;this.request=request;this.tokens=tokens;this.emit=emit;
    }
    public SnapshotTransfer(int chunkBytes,int maxBytes,long rpcNanos,IntSupplier leader,Function<Request,Frame> request,Supplier<DiskToken> tokens,Consumer<QuorumEffect> emit) {
        this(chunkBytes,maxBytes,leader,request,tokens,emit);if(rpcNanos<=0)throw new IllegalArgumentException("Invalid snapshot RPC timeout");this.rpcNanos=rpcNanos;
    }
    public void begin(SnapshotId id,long epoch) {
        cancel();this.id=id;this.epoch=epoch;source=leader.getAsInt();position=0;total=-1;lastChunk=null;
        var token=tokens.get();pending.put(token,result->send());emit.accept(new QuorumEffect.BeginDownload(token,id));
    }
    private void send(){if(id==null)return;flight=request.apply(new FetchSnapshot(epoch,id,position,chunkBytes));flightDeadline=now+rpcNanos;emit.accept(new QuorumEffect.Send(source,flight));}
    public boolean correlated(Frame frame){return flight!=null&&frame.requestId()==flight.requestId()&&frame.senderId()==source&&frame.operation()==105;}
    public boolean acceptFrame(Frame frame) {
        if(!correlated(frame))return false;
        if(frame.message() instanceof FetchSnapshotReply reply)accept(reply);else cancel();return true;
    }
    public void accept(FetchSnapshotReply reply) {
        if(id==null)return;
        if(lastChunk!=null&&lastChunk.id().equals(reply.id())&&lastChunk.position()==reply.position()
            &&lastChunk.totalLength()==reply.totalLength()&&Arrays.equals(lastChunk.chunk(),reply.chunk()))return;
        if(writing||!id.equals(reply.id())||reply.meta().epoch()!=epoch||reply.meta().error()!=QuorumError.NONE||reply.position()!=position
            ||reply.totalLength()<114||reply.totalLength()>Math.min(maxBytes,512+128*281)||total!=-1&&total!=reply.totalLength()
            ||reply.chunk().length==0||reply.chunk().length>chunkBytes||position>reply.totalLength()-reply.chunk().length){cancel();return;}
        total=reply.totalLength();lastChunk=reply;flight=null;writing=true;var token=tokens.get();
        pending.put(token,result->{
            writing=false;position=((DiskResult.ChunkWritten)result).end();
            if(position==total)finish();else send();
        });emit.accept(new QuorumEffect.WriteSnapshotChunk(token,id,position,reply.chunk()));
    }
    private void finish(){var token=tokens.get();pending.put(token,result->{var done=(DiskResult.DownloadFinished)result;installToken=tokens.get();pending.put(installToken,ignored->{id=null;installToken=null;});emit.accept(new QuorumEffect.InstallSnapshot(installToken,done.id(),done.image()));});emit.accept(new QuorumEffect.FinishDownload(token,id,total));}
    public boolean onCompletion(DiskDone done){var callback=pending.remove(done.token());if(callback==null)return false;if(!(done.result() instanceof DiskResult.Discarded))callback.accept(done.result());return true;}
    public void onTick(long now){this.now=now;if(flight!=null&&now>=flightDeadline)send();}
    public boolean installAllowed(DiskToken token){return token.equals(installToken);}
    public boolean active(){return id!=null;}
    public void cancel(){boolean had=id!=null;id=null;flight=null;installToken=null;writing=false;lastChunk=null;pending.clear();if(had)emit.accept(new QuorumEffect.CancelDownload(tokens.get()));}
}
