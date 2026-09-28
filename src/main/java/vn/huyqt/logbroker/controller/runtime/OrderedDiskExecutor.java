package vn.huyqt.logbroker.controller.runtime;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent;
import vn.huyqt.logbroker.controller.consensus.QuorumEvent.*;

/** FIFO disk worker reserves its completion slot before accepting work. */
public final class OrderedDiskExecutor implements AutoCloseable {
    private final ThreadPoolExecutor worker;private final ControllerLoop loop;private final AtomicInteger active=new AtomicInteger();
    public OrderedDiskExecutor(int capacity,ControllerLoop loop){this.loop=loop;worker=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(capacity),r->new Thread(r,"controller-disk"),new ThreadPoolExecutor.AbortPolicy());}
    public boolean submit(DiskToken token,Callable<DiskResult> work){
        var ticket=loop.reserveCompletion();if(ticket==null)return false;active.incrementAndGet();
        try{worker.execute(()->{QuorumEvent event;try{event=new DiskDone(token,work.call());}catch(Exception e){event=new DiskFailed(token,e.toString());}
            try{ticket.complete(event);}finally{active.decrementAndGet();}});return true;}
        catch(RejectedExecutionException e){active.decrementAndGet();ticket.close();return false;}
    }
    public boolean awaitIdle(Duration timeout)throws InterruptedException{long end=System.nanoTime()+timeout.toNanos();while(active.get()!=0&&System.nanoTime()<end)TimeUnit.MILLISECONDS.sleep(1);return active.get()==0;}
    public boolean stop(Duration timeout)throws InterruptedException{worker.shutdown();return worker.awaitTermination(timeout.toNanos(),TimeUnit.NANOSECONDS);}
    @Override public void close(){try{if(!stop(Duration.ofSeconds(30)))throw new IllegalStateException("Disk worker still active");}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}}
}
