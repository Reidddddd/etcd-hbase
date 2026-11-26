package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class OutboundAuthorizationHandoffTest {
  @Test
  void synchronousCompletionKeepsTheCurrentWriterAndIgnoresDuplicateCompletion() {
    var validations = new AtomicInteger();
    var resumes = new AtomicInteger();
    boolean handedOff = OutboundAuthorizationHandoff.start(completed -> {
      completed.accept(null);
      completed.accept(new IllegalStateException("duplicate"));
    }, failure -> {
      assertEquals(null, failure);
      validations.incrementAndGet();
    }, resumes::incrementAndGet);

    assertFalse(handedOff);
    assertEquals(1, validations.get());
    assertEquals(0, resumes.get());
  }

  @Test
  void asynchronousCompletionOwnsTheWriterExactlyOnce() {
    var completion = new AtomicReference<Consumer<Throwable>>();
    var validations = new AtomicInteger();
    var resumes = new AtomicInteger();
    var failure = new IllegalStateException("authorization rejected");
    assertTrue(OutboundAuthorizationHandoff.start(completion::set, observed -> {
      assertSame(failure, observed);
      validations.incrementAndGet();
    }, resumes::incrementAndGet));
    assertEquals(0, validations.get());
    assertEquals(0, resumes.get());

    completion.get().accept(failure);
    completion.get().accept(null);

    assertEquals(1, validations.get());
    assertEquals(1, resumes.get());
  }

  @Test
  void completionOnAnotherThreadBeforeHandoffStillUsesTheCurrentWriter() throws Exception {
    var validations = new AtomicInteger();
    var resumes = new AtomicInteger();
    var completedOn = new AtomicReference<Thread>();
    boolean handedOff = OutboundAuthorizationHandoff.start(completed -> {
      var thread = Thread.ofPlatform().start(() -> {
        completedOn.set(Thread.currentThread());
        completed.accept(null);
      });
      try {
        thread.join(5000);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(interrupted);
      }
      assertFalse(thread.isAlive());
    }, failure -> validations.incrementAndGet(), resumes::incrementAndGet);

    assertFalse(handedOff);
    assertFalse(Thread.currentThread().equals(completedOn.get()));
    assertEquals(1, validations.get());
    assertEquals(0, resumes.get());
  }

  @Test
  void startFailureDoesNotTransferOrResumeTheWriter() {
    var validations = new AtomicInteger();
    var resumes = new AtomicInteger();
    var failure = new IllegalStateException("validation could not start");
    assertSame(failure, assertThrows(IllegalStateException.class,
        () -> OutboundAuthorizationHandoff.start(completed -> { throw failure; },
            ignored -> validations.incrementAndGet(), resumes::incrementAndGet)));
    assertEquals(0, validations.get());
    assertEquals(0, resumes.get());
  }

  @Test
  void asynchronousValidationFailureStillResumesTheOwnerForTerminalCleanup() {
    var completion = new AtomicReference<Consumer<Throwable>>();
    var resumes = new AtomicInteger();
    var failure = new IllegalStateException("state update failed");
    assertTrue(OutboundAuthorizationHandoff.start(completion::set,
        ignored -> { throw failure; }, resumes::incrementAndGet));

    assertSame(failure, assertThrows(IllegalStateException.class,
        () -> completion.get().accept(null)));
    completion.get().accept(null);
    assertEquals(1, resumes.get());
  }
}
