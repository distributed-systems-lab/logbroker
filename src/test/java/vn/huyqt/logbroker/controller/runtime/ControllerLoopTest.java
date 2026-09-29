package vn.huyqt.logbroker.controller.runtime;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.controller.consensus.*;

class ControllerLoopTest {
    @Test void queuedSnapshotYieldsToControlDiskWorkWithoutReorderingMutations() throws Exception {
        var order=new CopyOnWriteArrayList<Integer>();var start=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var loop=new ControllerLoop(32,8,8,ignored->{},false);var disk=new OrderedDiskExecutor(8,loop)) {
            var token=new QuorumEvent.DiskToken(1,1,new UUID(0,1));
            assertTrue(disk.submit(token,()->{start.countDown();release.await();order.add(0);return new QuorumEvent.DiskResult.Flushed(0);}));
            assertTrue(start.await(2,TimeUnit.SECONDS));
            assertTrue(disk.submitLowPriority(token,()->{order.add(3);return new QuorumEvent.DiskResult.Flushed(0);}));
            assertTrue(disk.submit(token,()->{order.add(1);return new QuorumEvent.DiskResult.Flushed(0);}));
            assertTrue(disk.submit(token,()->{order.add(2);return new QuorumEvent.DiskResult.Flushed(0);}));
            release.countDown();assertTrue(disk.awaitIdle(Duration.ofSeconds(2)));loop.drain();assertEquals(List.of(0,1,2,3),order);
        } finally {release.countDown();}
    }
    @Test void timerCoalescesAndControlsYieldAfterEightEvents() {
        var events=new ArrayList<QuorumEvent>();
        try(var loop=new ControllerLoop(32,16,2,events::add,false)) {
            assertTrue(loop.submit(new QuorumEvent.Tick(1),ControllerLoop.Priority.CONTROL));
            assertTrue(loop.submit(new QuorumEvent.Tick(2),ControllerLoop.Priority.CONTROL));
            assertEquals(1,loop.queued());
            for(int i=0;i<10;i++)assertTrue(loop.submit(new QuorumEvent.Stop(),ControllerLoop.Priority.CONTROL));
            var general=new QuorumEvent.Tick(100);
            assertTrue(loop.submit(general,ControllerLoop.Priority.ADMIN));
            loop.drain();assertEquals(new QuorumEvent.Tick(2),events.getFirst());
            assertSame(general,events.get(8));
        }
    }
    @Test void fakeDiskSeparatesAppendFromForceAndPowerLoss() {
        var disk=new vn.huyqt.logbroker.controller.support.FakeDisk();
        var token=new QuorumEvent.DiskToken(1,1,new UUID(0,1));
        disk.enqueue(new QuorumEffect.Append(token,1,List.of(new vn.huyqt.logbroker.controller.log.QuorumEntry.ReadBarrier(1))));
        assertInstanceOf(QuorumEvent.DiskDone.class,disk.completeNext());
        assertEquals(1,disk.index().end());assertEquals(0,disk.durable());
        disk.powerLoss();assertEquals(0,disk.index().end());
        disk.enqueue(new QuorumEffect.PersistVote(token,1,0));disk.failNextForce();
        assertInstanceOf(QuorumEvent.DiskFailed.class,disk.completeNext());
        assertEquals(-1,disk.votedFor());
    }
    @Test void oldGenerationCompletionStillReleasesItsReservation() {
        var events=new ArrayList<QuorumEvent>();
        try(var loop=new ControllerLoop(8,2,1,events::add,false)) {
            var ticket=loop.reserveCompletion();
            ticket.complete(new QuorumEvent.DiskDone(new QuorumEvent.DiskToken(1,0,new UUID(0,999)),new QuorumEvent.DiskResult.Flushed(7)));
            loop.drain();assertEquals(0,loop.reservedCompletions());assertEquals(1,events.size());
        }
    }
    @Test void fullAdminQueueCannotConsumeControlOrReservedCompletionCapacity() throws Exception {
        var delivered=new ArrayList<QuorumEvent>();
        try(var loop=new ControllerLoop(8,2,2,delivered::add,false)) {
            for(int i=0;i<4;i++)assertTrue(loop.submit(new QuorumEvent.Tick(i),ControllerLoop.Priority.ADMIN));
            assertFalse(loop.submit(new QuorumEvent.Tick(5),ControllerLoop.Priority.ADMIN));
            assertTrue(loop.submit(new QuorumEvent.Tick(6),ControllerLoop.Priority.CONTROL));
            var ticket=loop.reserveCompletion();assertNotNull(ticket);
            ticket.complete(new QuorumEvent.Tick(7));loop.drain();
            assertEquals(6,delivered.size());assertEquals(0,loop.queued());
            assertEquals(0,loop.reservedCompletions());
        }
    }
    @Test void diskWorkIsOrderedAndRejectedBeforeRunningWithoutCompletionSpace() throws Exception {
        var done=new CopyOnWriteArrayList<QuorumEvent>();var started=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var loop=new ControllerLoop(8,2,1,done::add,false);
            var disk=new OrderedDiskExecutor(2,loop)) {
            var token=new QuorumEvent.DiskToken(1,0,new UUID(0,1));
            assertTrue(disk.submit(token,()->{started.countDown();assertTrue(release.await(2,TimeUnit.SECONDS));return new QuorumEvent.DiskResult.Flushed(1);}));
            assertTrue(started.await(2,TimeUnit.SECONDS));
            assertFalse(disk.submit(token,()->{fail("Rejected disk task executed");return null;}));
            release.countDown();assertTrue(disk.awaitIdle(Duration.ofSeconds(2)));loop.drain();
            assertInstanceOf(QuorumEvent.DiskDone.class,done.getFirst());
        } finally {release.countDown();}
    }
    @Test void completionCannotBePublishedTwiceAndCloseDoesNotDropActiveTicket() throws Exception {
        var events=new ArrayList<QuorumEvent>();var loop=new ControllerLoop(8,2,1,events::add,false);
        var ticket=loop.reserveCompletion();assertNotNull(ticket);
        assertThrows(IllegalStateException.class,loop::close);
        ticket.complete(new QuorumEvent.Tick(1));
        assertThrows(IllegalStateException.class,()->ticket.complete(new QuorumEvent.Tick(2)));
        loop.drain();loop.close();assertEquals(1,events.size());
    }
}
