package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Strong-read, exact-CAS persistence boundary for authentication-control records. */
public interface AuthSessionStore {
  CompletionStage<Optional<AuthSession>> readSession(ByteKey tokenDigest, RequestScope scope);

  CompletionStage<Boolean> createSessionIfAbsent(AuthSession session, RequestScope scope);

  /** Deletes only the exact observed session after it has been made unavailable. */
  CompletionStage<Boolean> deleteSessionIfUnchanged(AuthSession observed, RequestScope scope);

  CompletionStage<Boolean> compareAndSetSession(
      AuthSession observed, AuthSession replacement, RequestScope scope);

  CompletionStage<Optional<AuthSlot>> readSlot(int index, RequestScope scope);

  CompletionStage<Boolean> createSlotIfAbsent(AuthSlot slot, RequestScope scope);

  CompletionStage<Boolean> compareAndSetSlot(
      AuthSlot observed, AuthSlot replacement, RequestScope scope);

  CompletionStage<Optional<SubjectAuthority>> readAuthority(
      ByteKey subjectDigest, RequestScope scope);

  CompletionStage<Boolean> createAuthorityIfAbsent(
      SubjectAuthority authority, RequestScope scope);

  CompletionStage<Boolean> compareAndSetAuthority(
      SubjectAuthority observed, SubjectAuthority replacement, RequestScope scope);
}
