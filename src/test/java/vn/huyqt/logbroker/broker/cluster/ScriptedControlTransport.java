package vn.huyqt.logbroker.broker.cluster;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.ClusterIdentity;
import vn.huyqt.logbroker.controller.client.ControllerClientTransport;
import vn.huyqt.logbroker.controller.metadata.MetadataLimits;
import vn.huyqt.logbroker.controller.protocol.*;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
import vn.huyqt.logbroker.controller.protocol.BrokerControlProtocol.SenderRole;

final class ScriptedControlTransport implements ControllerClientTransport {
    static final ClusterIdentity ID = new ClusterIdentity(new UUID(0,1),1,List.of(
        new ClusterIdentity.Voter(1,"localhost",19001),new ClusterIdentity.Voter(2,"localhost",19002),
        new ClusterIdentity.Voter(3,"localhost",19003)),(short)2);
    static final class Factory implements ControllerClientTransport.Factory {
        final List<ScriptedControlTransport> connections = new ArrayList<>();
        public ControllerClientTransport create() { var wire=new ScriptedControlTransport(); connections.add(wire); return wire; }
        ScriptedControlTransport last() { return connections.getLast(); }
    }
    Consumer<Frame> receive;
    Consumer<Throwable> failure;
    final List<Frame> frames = new ArrayList<>();
    boolean closed;
    InetSocketAddress address;
    public CompletableFuture<Void> connect(InetSocketAddress address,Consumer<Frame> receive,Consumer<Throwable> failure) {
        this.address=address; this.receive=receive; this.failure=failure; return CompletableFuture.completedFuture(null);
    }
    public CompletableFuture<Void> send(Frame frame) {
        try { frames.add(QuorumCodec.decode(QuorumCodec.encode(frame),QuorumCodec.WireLimits.observer(MetadataLimits.defaults())));
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) { return CompletableFuture.failedFuture(e); }
    }
    List<Request> sent() { return frames.stream().map(f->(Request)f.message()).toList(); }
    void replyNext(Reply reply) { replyFrom(ID.clusterId(),reply); }
    void replyFrom(UUID cluster,Reply reply) {
        var sent=frames.getLast();
        try { receive.accept(QuorumCodec.decode(QuorumCodec.encode(new Frame((short)2,SenderRole.VOTER,
            sent.operation(),true,cluster,1,sent.requestId(),ID.voterHash(),reply)),
            QuorumCodec.WireLimits.observer(MetadataLimits.defaults()))); }
        catch (Exception e) { failure.accept(e); }
    }
    void disconnect() { failure.accept(new java.io.IOException("disconnect")); }
    public void close() { closed=true; }
}
