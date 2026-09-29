package vn.huyqt.logbroker.controller.transport;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;
public interface QuorumTransport {
    /** Receiver owns one reference; disk consumers retain it until their completion. */
    final class Inbound implements AutoCloseable {
        private final ReplyRoute route;private final Frame frame;private final Runnable release;private final BooleanSupplier active;private final AtomicInteger references=new AtomicInteger(1);
        public Inbound(ReplyRoute route,Frame frame,Runnable release){this(route,frame,release,()->true);}
        public Inbound(ReplyRoute route,Frame frame,Runnable release,BooleanSupplier active){this.route=route;this.frame=frame;this.release=release;this.active=active;}
        public Inbound(ReplyRoute route,Frame frame){this(route,frame,()->{});}
        public ReplyRoute route(){return route;}public Frame frame(){return frame;}
        public boolean isActive(){return active.getAsBoolean();}
        public Inbound retain(){int count;do{count=references.get();if(count<=0)throw new IllegalStateException("Inbound already released");}while(!references.compareAndSet(count,count+1));return this;}
        @Override public void close(){int left=references.decrementAndGet();if(left==0)release.run();if(left<0)throw new IllegalStateException("Inbound released twice");}
    }
    record ConnectionFailure(long connectionId,int peerId,Throwable failure){}
    InetSocketAddress start(InetSocketAddress bind,Consumer<Inbound> receiver,Consumer<Throwable> failure)throws IOException;
    CompletableFuture<Void> send(int peerId,Frame frame);
    CompletableFuture<Void> reply(ReplyRoute route,Frame frame);
    void stopAccepting();
    CompletableFuture<Void> closeAsync();
}
