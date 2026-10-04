package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.support.ManualScheduler;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class DeadlineSchedulerTest {
    @Test
    void cancelledTimerDoesNotRunAfterAdvancingClock() {
        try (var clock = new ManualScheduler()) {
            var calls = new AtomicInteger();
            var ticket =
                    clock.schedule(
                            clock.nanoTime() + Duration.ofMillis(10).toNanos(),
                            calls::incrementAndGet);
            assertTrue(ticket.cancel());
            clock.advance(Duration.ofMillis(11));
            clock.runDue();
            assertEquals(0, calls.get());
        }
    }
}
