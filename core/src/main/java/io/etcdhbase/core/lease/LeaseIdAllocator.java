package io.etcdhbase.core.lease;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Selects an ID against one pinned authoritative view while the owner holds its mutation lane. */
public final class LeaseIdAllocator {
  public record Selection(long id, Optional<LeaseState> previous) {
    public Selection {
      Objects.requireNonNull(previous, "previous");
      if (id == 0 || previous.filter(state -> state.identity().leaseId() != id).isPresent()) {
        throw new IllegalArgumentException("lease ID selection differs from its prior state");
      }
    }
  }

  private final LongSupplier candidates;
  private final int maximumAttempts;

  public LeaseIdAllocator(LongSupplier candidates, int maximumAttempts) {
    this.candidates = Objects.requireNonNull(candidates, "candidates");
    if (maximumAttempts < 1 || maximumAttempts > 128) {
      throw new IllegalArgumentException("lease ID attempt limit must be between 1 and 128");
    }
    this.maximumAttempts = maximumAttempts;
  }

  /** A nonzero requested ID is preserved, including its negative wire representation. */
  public CompletableFuture<Selection> select(long requested,
      Function<Long, CompletableFuture<Optional<LeaseState>>> find) {
    Objects.requireNonNull(find, "find");
    if (requested != 0) return lookup(requested, find);
    return selectAutomatic(find, 0);
  }

  private CompletableFuture<Selection> selectAutomatic(
      Function<Long, CompletableFuture<Optional<LeaseState>>> find, int attempt) {
    if (attempt >= maximumAttempts) {
      return CompletableFuture.failedFuture(new LeaseFailure(LeaseFailure.Reason.ID_SPACE_EXHAUSTED));
    }
    long candidate = candidates.getAsLong();
    if (candidate <= 0) return selectAutomatic(find, attempt + 1);
    return lookup(candidate, find).thenCompose(selection ->
        selection.previous().filter(state -> state.status() == LeaseState.Status.ACTIVE).isPresent()
            ? selectAutomatic(find, attempt + 1) : CompletableFuture.completedFuture(selection));
  }

  private static CompletableFuture<Selection> lookup(long id,
      Function<Long, CompletableFuture<Optional<LeaseState>>> find) {
    return Objects.requireNonNull(find.apply(id), "lease lookup future")
        .thenApply(previous -> new Selection(id, previous));
  }
}
