package io.etcdhbase.grpc;

import io.grpc.Metadata;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

/** Reads the etcd client's bearer token metadata without exposing it in diagnostics. */
public final class EtcdAuthTokenMetadata {
  private static final Metadata.Key<String> TOKEN =
      Metadata.Key.of("token", Metadata.ASCII_STRING_MARSHALLER);

  private EtcdAuthTokenMetadata() {}

  /** Returns the single token value, rejecting duplicate metadata instead of choosing one. */
  public static Optional<String> read(Metadata metadata) {
    Objects.requireNonNull(metadata, "metadata");
    Iterable<String> values = metadata.getAll(TOKEN);
    if (values == null) return Optional.empty();
    Iterator<String> iterator = values.iterator();
    if (!iterator.hasNext()) return Optional.empty();
    String token = iterator.next();
    if (iterator.hasNext()) {
      throw new IllegalArgumentException("authentication token metadata must be unique");
    }
    return Optional.of(token);
  }

  /** Metadata key used by etcd clients when attaching the Authenticate response token. */
  public static Metadata.Key<String> key() { return TOKEN; }
}
