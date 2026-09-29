package vn.huyqt.logbroker.controller;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import vn.huyqt.logbroker.controller.persistence.*;
import vn.huyqt.logbroker.controller.consensus.QuorumStatus;
import vn.huyqt.logbroker.controller.support.FaultFiles;
class ControllerNodeClusterTest {
    @TempDir Path root;
    @Test void realTcpNodeCompositionSnapshotsAndKeepsUserCallbacksOffConsensusLoop() throws Exception {
        var nodes=new ArrayList<ControllerNode>();var sockets=new ArrayList<java.net.ServerSocket>();var voters=new ArrayList<ClusterIdentity.Voter>();
        for(int id=0;id<3;id++){var socket=new java.net.ServerSocket(0);sockets.add(socket);voters.add(new ClusterIdentity.Voter(id,"127.0.0.1",socket.getLocalPort()));}
        for(var socket:sockets)socket.close();var release=new CountDownLatch(1);var started=new CountDownLatch(1);
        try {
            for(int id=0;id<3;id++){
                var identity=new ClusterIdentity(new UUID(0,123),id,voters);var files=new FaultFiles();Path directory=root.resolve("node-"+id);QuorumStateStore.format(directory,identity,files);
                var config=ControllerConfig.builder(identity).fetchIdleWait(Duration.ofMillis(20)).rpcTimeout(Duration.ofMillis(100)).electionMin(Duration.ofMillis(200)).electionMax(Duration.ofMillis(500)).snapshotTriggerBytes(1).shutdownTimeout(Duration.ofSeconds(5)).build();
                var node=ControllerNode.open(directory,config,files);nodes.add(node);node.start();
            }
            ControllerNode leader=null;long limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(leader==null&&System.nanoTime()<limit){for(var node:nodes)if(node.status().get().ready())leader=node;if(leader==null)Thread.sleep(10);}
            assertNotNull(leader,()->"No ready leader: "+nodes.stream().map(n->n.status().join()).toList());
            var create=leader.service().createTopic("orders",3,System.nanoTime()+TimeUnit.SECONDS.toNanos(5));
            var callback=create.thenRun(()->{started.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}});
            assertNotNull(create.get(5,TimeUnit.SECONDS));assertTrue(started.await(2,TimeUnit.SECONDS));
            assertEquals(QuorumStatus.Role.LEADER,leader.status().get(1,TimeUnit.SECONDS).role());
            var metadata=leader.service().readMetadata(System.nanoTime()+TimeUnit.SECONDS.toNanos(5)).get(5,TimeUnit.SECONDS);assertEquals("orders",metadata.topics().getFirst().name());
            release.countDown();callback.get(2,TimeUnit.SECONDS);
            limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);while(leader.status().join().snapshotEnd()==0&&System.nanoTime()<limit)Thread.sleep(10);assertTrue(leader.status().join().snapshotEnd()>0);
        } finally {release.countDown();for(var node:nodes)node.close();}
    }
}
