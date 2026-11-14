package io.etcdhbase.core.commit;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A captured immutable read view whose retention pin is owned by the request scope. */
public final class PinnedReadView implements AutoCloseable {
  private final ReadView view;
  private final ReadViewPinRegistry.Pin pin;
  private final RequestScope scope;
  private final boolean auxiliaryProtected;
  private final CompletableFuture<Void> cleanupCompletion = new CompletableFuture<>();
  private final CompletableFuture<Void> termination = new CompletableFuture<>();
  private boolean closeRequested;
  private boolean pinClosed;
  private boolean completionInProgress;
  private boolean releaseStarted;
  private int activeReaders;

  PinnedReadView(ReadView view, ReadViewPinRegistry.Pin pin, RequestScope scope) {
    this(view, pin, scope, false);
  }

  PinnedReadView(ReadView view, ReadViewPinRegistry.Pin pin, RequestScope scope,
      boolean auxiliaryProtected) {
    this.view = Objects.requireNonNull(view, "view");
    this.pin = Objects.requireNonNull(pin, "pin");
    this.scope = Objects.requireNonNull(scope, "scope");
    this.auxiliaryProtected = auxiliaryProtected;
    if (view.pinGeneration() != pin.generation()) {
      throw new IllegalArgumentException("read view and pin generations differ");
    }
  }

  public ReadView view() { return view; }
  public boolean belongsTo(RequestScope requestScope) { return scope == requestScope; }
  public boolean protectsAuxiliary() { return auxiliaryProtected; }
  public Optional<ManifestRoot> selectedRoot() { return view.selectedRoot(); }
  public synchronized boolean isOpen() { return !closeRequested && !pinClosed; }
  CompletionStage<Void> cleanupCompletion() { return cleanupCompletion.minimalCompletionStage(); }
  public CompletionStage<Void> termination() { return termination.minimalCompletionStage(); }

  CompletableFuture<Void> renew(RequestScope scope) {
    synchronized (this) {
      if (closeRequested || pinClosed || activeReaders == 0) {
        return CompletableFuture.failedFuture(cancelled());
      }
    }
    return Objects.requireNonNull(pin.renew(scope), "pin renewal future");
  }

  CompletableFuture<Void> complete(RequestScope scope) {
    synchronized (this) {
      if (pinClosed) return CompletableFuture.completedFuture(null);
      if (closeRequested || activeReaders != 1 || completionInProgress) {
        return CompletableFuture.failedFuture(cancelled());
      }
      closeRequested = true;
      completionInProgress = true;
    }
    CompletableFuture<Void> completion;
    try {
      completion = Objects.requireNonNull(pin.complete(scope), "pin completion future");
    } catch (RuntimeException | Error failure) {
      // Restore the state guard before a fatal Error escapes; readers may still need to release.
      synchronized (this) { completionInProgress = false; }
      if (failure instanceof Error fatal) throw fatal;
      return CompletableFuture.failedFuture(failure);
    }
    return completion.whenComplete((ignored, failure) -> {
      boolean completed;
      boolean release;
      synchronized (PinnedReadView.this) {
        completionInProgress = false;
        if (failure == null) pinClosed = true;
        completed = pinClosed && activeReaders == 0;
        release = closeRequested && activeReaders == 0;
      }
      if (completed) termination.complete(null);
      if (release) releasePinIfReady();
    });
  }

  synchronized ReaderLease retainReader() {
    if (closeRequested || pinClosed) {
      throw new IllegalStateException("read view pin has already been released");
    }
    activeReaders = Math.addExact(activeReaders, 1);
    return new ReaderLease(this);
  }

  @Override
  public void close() {
    synchronized (this) {
      closeRequested = true;
    }
    // Seal the producer set before waiting for it; cancelled request futures may finish earlier.
    scope.close();
    releasePinIfReady();
  }

  private void releaseReader() {
    boolean completed;
    boolean release;
    synchronized (this) {
      if (activeReaders == 0) throw new IllegalStateException("read view reader lease underflow");
      activeReaders--;
      completed = pinClosed && activeReaders == 0;
      release = closeRequested && activeReaders == 0 && !completionInProgress;
    }
    if (completed) termination.complete(null);
    if (release) releasePinIfReady();
  }

  private void releasePinIfReady() {
    synchronized (this) {
      if (!closeRequested || activeReaders != 0 || completionInProgress || releaseStarted) return;
      releaseStarted = true;
    }
    scope.physicalProducersCompletion().whenComplete((ignored, producerFailure) -> {
      if (producerFailure != null) {
        termination.completeExceptionally(producerFailure);
        cleanupCompletion.completeExceptionally(producerFailure);
        return;
      }
      boolean releasePin;
      synchronized (PinnedReadView.this) {
        releasePin = !pinClosed;
        pinClosed = true;
      }
      if (!releasePin) {
        termination.complete(null);
        cleanupCompletion.complete(null);
        return;
      }
      try {
        pin.close();
      } catch (RuntimeException | Error failure) {
        // Settle both lifecycle observers before rethrowing a fatal Error from pin cleanup.
        termination.completeExceptionally(failure);
        cleanupCompletion.completeExceptionally(failure);
        if (failure instanceof Error fatal) throw fatal;
        return;
      }
      pin.releaseCompletion().whenComplete((released, failure) -> {
        if (failure == null) {
          termination.complete(null);
          cleanupCompletion.complete(null);
        } else {
          termination.completeExceptionally(failure);
          cleanupCompletion.completeExceptionally(failure);
        }
      });
    });
  }

  private static BackendFailure cancelled() {
    return new BackendFailure(BackendFailure.Category.CANCELLED);
  }

  static final class ReaderLease implements AutoCloseable {
    private final PinnedReadView owner;
    private boolean closed;

    private ReaderLease(PinnedReadView owner) { this.owner = owner; }

    @Override
    public void close() {
      synchronized (this) {
        if (closed) return;
        closed = true;
      }
      owner.releaseReader();
    }
  }
}
