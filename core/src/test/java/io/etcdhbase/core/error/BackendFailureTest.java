package io.etcdhbase.core.error;

import java.io.IOException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class BackendFailureTest {
  @Test
  void retainsCauseAndUsesOnlySafeCategoryText() {
    IOException cause = new IOException("password=must-not-appear");
    BackendFailure failure = new BackendFailure(BackendFailure.Category.AUTHENTICATION, cause);

    assertEquals(BackendFailure.Category.AUTHENTICATION, failure.category());
    assertSame(cause, failure.getCause());
    assertEquals("backend authentication failed", failure.getMessage());
    assertEquals(failure.getMessage(), failure.safeMessage());
  }
}
