package vn.huyqt.logbroker.controller.runtime;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.ArrayDeque;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;

/** Mutations stay FIFO; independent snapshot serialization uses a bounded lower-priority queue. */
public final class OrderedDiskExecutor implements AutoCloseable {
    private record Work(DiskToken token,Callable<DiskResult> callable,ControllerLoop.CompletionTicket ticket){}
    private final Thread worker;private final ControllerLoop loop;private final int capacity;
    private final ArrayDeque<Work> normal=new ArrayDeque<>(),low=new ArrayDeque<>();
    private int active,normalTurns;private boolean stopping;
    public OrderedDiskExecutor(int capacity,ControllerLoop loop){if(capacity<1)throw new IllegalArgumentException("Invalid disk capacity");this.capacity=capacity;this.loop=loop;worker=new Thread(this::run,"controller-disk");worker.start();}
    public boolean submit(DiskToken token,Callable<DiskResult> work){return admit(token,work,false);}
    public synchronized boolean submitOrdinary(DiskToken token,Callable<DiskResult> work){if(loop.reservedCompletions()>=capacity-Math.min(16,capacity/2))return false;return admit(token,work,false);}
    public boolean submitLowPriority(DiskToken token,Callable<DiskResult> work){return admit(token,work,true);}
    private synchronized boolean admit(DiskToken token,Callable<DiskResult> work,boolean lowPriority){
        if(lowPriority&&loop.reservedCompletions()>=capacity-Math.min(16,capacity/2))return false;
        if(stopping||normal.size()+low.size()>=capacity)return false;
        var ticket=loop.reserveCompletion();if(ticket==null)return false;
        (lowPriority?low:normal).addLast(new Work(token,work,ticket));active++;notifyAll();return true;
    }
    private synchronized Work next()throws InterruptedException {
        while(normal.isEmpty()&&low.isEmpty()&&!stopping)wait();
        if(!low.isEmpty()&&(normal.isEmpty()||normalTurns>=8)){normalTurns=0;return low.removeFirst();}
        if(!normal.isEmpty()){normalTurns++;return normal.removeFirst();}return null;
    }
    private void run(){
        try {Work work;while((work=next())!=null){QuorumEvent event;
            try{event=new DiskDone(work.token(),work.callable().call());}catch(Exception e){event=new DiskFailed(work.token(),e.toString());}
            try{work.ticket().complete(event);}finally{synchronized(this){active--;notifyAll();}}
        }}catch(InterruptedException e){Thread.currentThread().interrupt();}
    }
    public synchronized boolean awaitIdle(Duration timeout)throws InterruptedException{long end=System.nanoTime()+timeout.toNanos(),remaining;while(active!=0&&(remaining=end-System.nanoTime())>0)TimeUnit.NANOSECONDS.timedWait(this,remaining);return active==0;}
    public boolean stop(Duration timeout)throws InterruptedException{synchronized(this){stopping=true;notifyAll();}worker.join(Math.max(1,timeout.toMillis()));return !worker.isAlive();}
    @Override public void close(){try{if(!stop(Duration.ofSeconds(30)))throw new IllegalStateException("Disk worker still active");}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}}
}
