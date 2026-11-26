package io.etcdhbase.core.commit;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Durable request binding and exact control attempt for logical Compact selection. */
public record CompactionOperation(InvocationId id, ByteKey requestDigest, ByteKey authorizationDigest,
    ByteKey profile, long revision, long responseRevision, long ownerEpoch, long baseSequence,
    long controlVersion, ByteKey controlDigest, Stage stage) {
  public enum Stage { PREPARED, LOGICAL, COMPACTED, FUTURE }
  private static final int FORMAT = 1;
  public static final int ENCODED_BYTES = 4 + 16 + 4 * 32 + 5 * Long.BYTES + 4;

  public CompactionOperation {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(stage, "stage");
    if (requestDigest.length() != 32 || authorizationDigest.length() != 32 || profile.length() != 32
        || controlDigest.length() != 32 || responseRevision <= 0 || ownerEpoch <= 0
        || baseSequence < 0 || controlVersion <= 0 || stage == Stage.LOGICAL && revision < 0) {
      throw new IllegalArgumentException("invalid Compact operation");
    }
  }

  CompactionOperation forAttempt(ControlSnapshot observed) {
    if (stage != Stage.PREPARED) throw new IllegalStateException("Compact is already resolved");
    CommitState state = observed.state();
    return new CompactionOperation(id, requestDigest, authorizationDigest, profile, revision,
        state.publishedRevision(), ownerEpoch, state.publishedSequence(), state.stateVersion(),
        ManifestDigest.sha256(observed.comparisonToken()), stage);
  }

  CompactionOperation transition(Stage next) {
    if (stage != Stage.PREPARED || next == Stage.PREPARED) {
      throw new IllegalStateException("invalid Compact transition");
    }
    return new CompactionOperation(id, requestDigest, authorizationDigest, profile, revision,
        responseRevision, ownerEpoch, baseSequence, controlVersion, controlDigest, next);
  }

  public ByteKey encode() {
    return ByteKey.copyOf(ByteBuffer.allocate(ENCODED_BYTES).putInt(FORMAT).put(id.bytes())
        .put(requestDigest.bytes()).put(authorizationDigest.bytes()).put(profile.bytes())
        .putLong(revision).putLong(responseRevision).putLong(ownerEpoch).putLong(baseSequence)
        .putLong(controlVersion).put(controlDigest.bytes()).putInt(stage.ordinal()).array());
  }

  public static CompactionOperation decode(ByteKey bytes) {
    if (bytes.length() != ENCODED_BYTES) throw new IllegalArgumentException("invalid Compact record size");
    ByteBuffer input = ByteBuffer.wrap(bytes.bytes());
    if (input.getInt() != FORMAT) throw new IllegalArgumentException("unknown Compact format");
    byte[] id = new byte[16];
    input.get(id);
    ByteKey request = digest(input);
    ByteKey authorization = digest(input);
    ByteKey profile = digest(input);
    long revision = input.getLong();
    long response = input.getLong();
    long epoch = input.getLong();
    long sequence = input.getLong();
    long version = input.getLong();
    ByteKey control = digest(input);
    int stage = input.getInt();
    if (stage < 0 || stage >= Stage.values().length) throw new IllegalArgumentException("unknown Compact stage");
    return new CompactionOperation(InvocationId.fromBytes(id), request, authorization, profile,
        revision, response, epoch, sequence, version, control, Stage.values()[stage]);
  }

  private static ByteKey digest(ByteBuffer input) {
    byte[] value = new byte[32];
    input.get(value);
    return ByteKey.copyOf(value);
  }
}
