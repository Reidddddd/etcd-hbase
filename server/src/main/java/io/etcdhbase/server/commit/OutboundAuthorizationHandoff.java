package io.etcdhbase.server.commit;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** Transfers the exclusive stream writer to an asynchronous authorization completion, without a queue. */
final class OutboundAuthorizationHandoff {
  private sealed interface State permits Phase, Completed {}
  private enum Phase implements State { STARTING, HANDED_OFF }
  private record Completed(Throwable failure) implements State {}

  private OutboundAuthorizationHandoff() {}

  /**
   * The caller already owns the writer. A false return leaves it with the caller after validation;
   * a true return transfers it to the completion, which must resume and eventually release it.
   * A completion before the handoff is consumed by the current writer rather than recursively
   * resuming it. Duplicate completions cannot acquire the writer a second time.
   */
  static boolean start(Consumer<Consumer<Throwable>> startValidation,
      Consumer<Throwable> validated, Runnable resumeWriter) {
    var state = new AtomicReference<State>(Phase.STARTING);
    startValidation.accept(failure -> {
      var completed = new Completed(failure);
      if (state.compareAndSet(Phase.STARTING, completed)) return;
      if (state.compareAndSet(Phase.HANDED_OFF, completed)) {
        try {
          validated.accept(failure);
        } finally {
          resumeWriter.run();
        }
      }
    });
    if (state.compareAndSet(Phase.STARTING, Phase.HANDED_OFF)) return true;
    validated.accept(((Completed) state.get()).failure());
    return false;
  }
}
