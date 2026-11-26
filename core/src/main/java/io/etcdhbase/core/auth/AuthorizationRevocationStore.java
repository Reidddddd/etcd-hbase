package io.etcdhbase.core.auth;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.request.RequestScope;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Durable operation snapshot and progress; an exceptional write has an unknown outcome. */
public interface AuthorizationRevocationStore {
  CompletionStage<Optional<AuthorizationRevocation>> readRevocation(ByteKey operationId, RequestScope scope);

  CompletionStage<Boolean> createRevocationIfAbsent(AuthorizationRevocation initial, RequestScope scope);

  CompletionStage<Boolean> compareAndSetRevocation(
      AuthorizationRevocation observed, AuthorizationRevocation replacement, RequestScope scope);
}
