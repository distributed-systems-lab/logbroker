package vn.huyqt.logbroker.controller.transport;
import static org.junit.jupiter.api.Assertions.*;import java.net.*;import java.util.*;import java.util.concurrent.*;import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.broker.*;import vn.huyqt.logbroker.controller.*;import vn.huyqt.logbroker.controller.log.*;import vn.huyqt.logbroker.controller.protocol.*;import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;import vn.huyqt.logbroker.controller.support.ControllerTestSupport;
class QuorumOutboundOwnershipTest {
    @Test void queuedReplyDtosReserveBytesBeforeValidationAndReleaseOnClose()throws Exception {
        var identity=ControllerTestSupport.identity(0);var received=new LinkedBlockingQueue<QuorumTransport.Inbound>();var release=new CountDownLatch(1);var blocked=new CountDownLatch(2);
        try(var clock=DeadlineScheduler.system()){var transport=new NettyQuorumTransport(ControllerConfig.defaults(identity),clock);try{var bind=transport.start(new InetSocketAddress("127.0.0.1",0),inbound->{received.add(inbound);inbound.close();},ignored->{});
            try(var socket=new Socket(bind.getAddress(),bind.getPort())){for(int i=0;i<20;i++)socket.getOutputStream().write(QuorumCodec.encode(new Frame((short)104,false,identity.clusterId(),1,i,identity.voterHash(),new QuorumFetch(1,0,0,4*1024*1024,100,0))));socket.getOutputStream().flush();
                var inbound=new ArrayList<QuorumTransport.Inbound>();for(int i=0;i<20;i++)inbound.add(Objects.requireNonNull(received.poll(3,TimeUnit.SECONDS)));
                Runnable hold=()->{blocked.countDown();try{release.await();}catch(InterruptedException error){Thread.currentThread().interrupt();}};assertTrue(transport.validationTask(true,hold));assertTrue(transport.validationTask(true,hold));assertTrue(blocked.await(2,TimeUnit.SECONDS));
                var batch=new QuorumBatch(0,Collections.nCopies(20_000,new QuorumEntry.ReadBarrier(1)));var futures=new ArrayList<CompletableFuture<Void>>();for(var request:inbound)futures.add(transport.reply(request.route(),new Frame((short)104,true,identity.clusterId(),0,request.frame().requestId(),identity.voterHash(),new QuorumFetchReply(new ReplyMeta(QuorumError.NONE,"",1,0),1,0,new FetchData(List.of(batch))))));
                assertTrue(transport.outboundUsed()>0,"Queued DTOs are uncharged before encoding");assertTrue(transport.outboundUsed()<=64L*1024*1024);assertTrue(futures.stream().anyMatch(CompletableFuture::isCompletedExceptionally),"Byte pressure must reject before unlimited queueing");
            }
        }finally{release.countDown();transport.closeAsync().get(5,TimeUnit.SECONDS);}assertEquals(0,transport.outboundUsed());assertEquals(0,transport.inboundUsed());}
    }
}
