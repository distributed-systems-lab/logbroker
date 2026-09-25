package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import vn.huyqt.logbroker.protocol.Protocol.TopicPartition;

class PartitionExecutorTest {
    private static final TopicPartition A = new TopicPartition(new UUID(1, 1), 0);
    private static final TopicPartition B = new TopicPartition(new UUID(1, 1), 1);

    @Test
    void serializesEachPartitionWithoutBlockingAnother() throws Exception {
        try (var executor = new PartitionExecutor(2, 2, 2)) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var first = executor.submit(A, () -> {
                entered.countDown(); release.await(); return 1;
            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var second = executor.submit(A, () -> 3);
            assertFalse(second.isDone());
            assertEquals(2, executor.submit(B, () -> 2).get(2, TimeUnit.SECONDS));
            release.countDown();
            assertEquals(1, first.get(2, TimeUnit.SECONDS));
            assertEquals(3, second.get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void dueControlRunsBeforeNextUserTaskAndUserQueueIsBounded() throws Exception {
        try (var executor = new PartitionExecutor(1, 1, 1)) {
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            List<String> order = java.util.Collections.synchronizedList(new ArrayList<>());
            var first = executor.submit(A, () -> {
                entered.countDown(); release.await(); order.add("first"); return 1;
            });
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            var second = executor.submit(A, () -> { order.add("second"); return 2; });
            assertThrows(RejectedExecutionException.class,
                    () -> executor.submit(A, () -> 3));
            executor.control(A, () -> order.add("flush"));
            release.countDown();
            first.get(2, TimeUnit.SECONDS);
            second.get(2, TimeUnit.SECONDS);
            assertEquals(List.of("first", "flush", "second"), order);
        }
    }
}
