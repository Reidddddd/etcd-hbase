package io.etcdhbase.core.error;

import java.util.Objects;

/** A backend failure with a bounded category and a safe message that does not expose its cause. */
public final class BackendFailure extends Exception {
  private static final long serialVersionUID = 1L;

  private final Category category;

  public BackendFailure(Category category) {
    this(category, null);
  }

  public BackendFailure(Category category, Throwable cause) {
    super(Objects.requireNonNull(category, "category").safeMessage(), cause);
    this.category = category;
  }

  public Category category() {
    return category;
  }

  public String safeMessage() {
    return category.safeMessage();
  }

  public enum Category {
    AUTHENTICATION("backend authentication failed"),
    PERMISSION("backend permission was denied"),
    UNAVAILABLE("backend is unavailable"),
    TIMEOUT("backend operation timed out"),
    CANCELLED("backend operation was cancelled"),
    RESOURCE_LIMIT("backend resource limit was reached"),
    UNKNOWN("backend operation failed");

    private final String safeMessage;

    Category(String safeMessage) {
      this.safeMessage = safeMessage;
    }

    private String safeMessage() {
      return safeMessage;
    }
  }
}
