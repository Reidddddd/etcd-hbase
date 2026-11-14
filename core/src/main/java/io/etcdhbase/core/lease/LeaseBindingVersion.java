package io.etcdhbase.core.lease;

import io.etcdhbase.core.bytes.ByteKey;
import java.util.Objects;

/** Immutable binding state tied to the KV version that owns the key at one commit sequence. */
public record LeaseBindingVersion(LeaseIdentity identity, ByteKey key, boolean bound,
    long sequence, long keyCreateRevision, long keyModRevision) {
  public LeaseBindingVersion {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(key, "key");
    if (key.length() == 0 || sequence <= 0 || keyCreateRevision <= 0
        || keyModRevision < keyCreateRevision) {
      throw new IllegalArgumentException("invalid Lease binding version");
    }
  }

  public LeaseBindingIdentity recordIdentity() {
    return new LeaseBindingIdentity(identity.leaseId(), identity.incarnation(), key, sequence);
  }
}
