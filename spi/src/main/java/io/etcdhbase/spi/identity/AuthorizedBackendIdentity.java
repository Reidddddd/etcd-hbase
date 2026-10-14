package io.etcdhbase.spi.identity;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Immutable, provider-issued backend authorization handle.
 *
 * <p>The context reference is opaque and must not contain credential material. The allowed table
 * groups are immutable and must include the table group in the authentication scope. Sensitive
 * identity fields are intentionally omitted from {@link #toString()}.
 */
public final class AuthorizedBackendIdentity {
  private static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9._-]{0,63}");
  private static final int MAX_SUBJECT_CODE_POINTS = 256;
  private static final int MAX_CONTEXT_REF_CODE_POINTS = 512;

  private final String providerId;
  private final String subject;
  private final String contextRef;
  private final AuthenticationScope scope;
  private final Set<String> allowedTableGroupIds;
  private final long credentialGeneration;

  public AuthorizedBackendIdentity(
      String providerId,
      String subject,
      String contextRef,
      AuthenticationScope scope,
      Set<String> allowedTableGroupIds,
      long credentialGeneration) {
    this.providerId = requireId(providerId, "providerId");
    this.subject = requireBoundedText(subject, "subject", MAX_SUBJECT_CODE_POINTS);
    this.contextRef = requireBoundedText(contextRef, "contextRef", MAX_CONTEXT_REF_CODE_POINTS);
    this.scope = Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(allowedTableGroupIds, "allowedTableGroupIds");
    Set<String> groups = Set.copyOf(allowedTableGroupIds);
    if (groups.isEmpty()
        || groups.stream().anyMatch(groupId -> !ID_PATTERN.matcher(groupId).matches())) {
      throw new IllegalArgumentException("allowedTableGroupIds must contain valid configured IDs");
    }
    if (!groups.contains(scope.tableGroupId())) {
      throw new IllegalArgumentException(
          "allowedTableGroupIds must include the requested table group");
    }
    if (credentialGeneration < 0) {
      throw new IllegalArgumentException("credentialGeneration must not be negative");
    }
    this.allowedTableGroupIds = groups;
    this.credentialGeneration = credentialGeneration;
  }

  public String providerId() {
    return providerId;
  }

  public String subject() {
    return subject;
  }

  /** Provider-owned opaque context lookup key; it is not a credential. */
  public String contextRef() {
    return contextRef;
  }

  public AuthenticationScope scope() {
    return scope;
  }

  public Set<String> allowedTableGroupIds() {
    return allowedTableGroupIds;
  }

  public long credentialGeneration() {
    return credentialGeneration;
  }

  @Override
  public String toString() {
    return "AuthorizedBackendIdentity[providerId="
        + providerId
        + ", tableGroupId="
        + scope.tableGroupId()
        + ", credentialGeneration="
        + credentialGeneration
        + ", subject=redacted, contextRef=redacted]";
  }

  private static String requireId(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!ID_PATTERN.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " must be a valid configured identifier");
    }
    return value;
  }

  private static String requireBoundedText(String value, String name, int maxCodePoints) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()
        || value.codePointCount(0, value.length()) > maxCodePoints
        || value.codePoints().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException(name + " must be bounded non-control text");
    }
    return value;
  }
}
