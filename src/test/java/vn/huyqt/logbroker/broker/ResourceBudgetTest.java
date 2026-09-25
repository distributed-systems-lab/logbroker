package vn.huyqt.logbroker.broker;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ResourceBudgetTest {
    @Test
    void reservationNeverExceedsLimitAndReleaseIsIdempotent() {
        var budget = new ResourceBudget(10);
        var lease = budget.reserve(7).orElseThrow();
        assertTrue(budget.reserve(4).isEmpty());
        lease.close();
        lease.close();
        assertEquals(0, budget.used());
        assertTrue(budget.reserve(10).isPresent());
    }

    @Test
    void rejectsOverflowSizedReservation() {
        var budget = new ResourceBudget(Long.MAX_VALUE);
        var lease = budget.reserve(Long.MAX_VALUE - 1).orElseThrow();
        assertTrue(budget.reserve(3).isEmpty());
        lease.close();
    }
}
