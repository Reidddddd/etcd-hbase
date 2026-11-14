package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LeaseIdAllocatorTest {
  @Test
  void checksCollisionsAndReturnsTheRetiredVersionForIncarnationFencing() {
    var active = LeaseState.grant(new LeaseIdentity(1, 1), 10, 20_000, 1);
    var retired = LeaseState.grant(new LeaseIdentity(2, 4), 10, 20_000, 1).revoke(2);
    var next = new AtomicInteger();
    var allocator = new LeaseIdAllocator(next::incrementAndGet, 3);
    var selection = allocator.select(0, id -> CompletableFuture.completedFuture(
        id == 1 ? Optional.of(active) : Optional.of(retired))).join();
    assertEquals(2, selection.id());
    assertEquals(retired, selection.previous().orElseThrow());
  }

  @Test
  void preservesNegativeExplicitIdWithoutCallingGenerator() {
    var allocator = new LeaseIdAllocator(() -> { throw new AssertionError("unexpected allocation"); }, 1);
    assertEquals(Long.MIN_VALUE,
        allocator.select(Long.MIN_VALUE, id -> CompletableFuture.completedFuture(Optional.empty()))
            .join().id());
  }

  @Test
  void failsAfterBoundedCollisionsAndPropagatesLookupFailures() {
    var active = LeaseState.grant(new LeaseIdentity(1, 1), 10, 20_000, 1);
    var allocator = new LeaseIdAllocator(() -> 1, 2);
    var exhausted = assertThrows(CompletionException.class, () -> allocator.select(0,
        id -> CompletableFuture.completedFuture(Optional.of(active))).join());
    assertEquals(LeaseFailure.Reason.ID_SPACE_EXHAUSTED, ((LeaseFailure) exhausted.getCause()).reason());
    var lookupFailure = new IllegalStateException("read failed");
    var failure = assertThrows(CompletionException.class, () -> allocator.select(0,
        id -> CompletableFuture.failedFuture(lookupFailure)).join());
    assertEquals(lookupFailure, failure.getCause());
  }

  @Test
  void rejectsMismatchedStateAndCountsInvalidCandidatesAgainstTheLimit() {
    var allocator = new LeaseIdAllocator(() -> -1, 1);
    assertEquals(LeaseFailure.Reason.ID_SPACE_EXHAUSTED,
        ((LeaseFailure) assertThrows(CompletionException.class, () -> allocator.select(0,
            id -> { throw new AssertionError("invalid candidate was read"); }).join()).getCause()).reason());
    var other = LeaseState.grant(new LeaseIdentity(2, 1), 10, 20_000, 1);
    assertThrows(CompletionException.class, () -> allocator.select(1,
        id -> CompletableFuture.completedFuture(Optional.of(other))).join());
  }
}
