package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.auth.AuthSession;
import io.etcdhbase.core.auth.AuthSlot;
import io.etcdhbase.core.auth.CredentialEnvelope;
import io.etcdhbase.core.auth.SubjectAuthority;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.auth.proto.AuthProtos;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.util.Objects;

/** Strict codec for the private auth-control protobuf schema. */
public final class AuthMessages {
  public static final int FORMAT_VERSION = 1;
  public static final int MAX_RECORD_BYTES = 128 * 1024;

  private AuthMessages() {}

  public static byte[] encode(AuthSession session) {
    Objects.requireNonNull(session, "session");
    var sourceEnvelope = session.credentialEnvelope();
    var envelope = AuthProtos.CredentialEnvelope.newBuilder()
        .setKeyId(sourceEnvelope.keyId()).setAadVersion(sourceEnvelope.aadVersion())
        .setNonce(ByteString.copyFrom(sourceEnvelope.nonce()))
        .setCiphertext(ByteString.copyFrom(sourceEnvelope.ciphertext()));
    var encoded = AuthProtos.AuthSessionRecord.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setSessionId(bytes(session.sessionId()))
        .setTokenDigest(bytes(session.tokenDigest()))
        .setSubjectDigest(bytes(session.subjectDigest()))
        .setTrustDomain(session.trustDomain())
        .setProviderId(session.providerId())
        .setTableGroupId(session.scope().tableGroupId())
        .setConnectionProfileId(session.scope().connectionProfileId())
        .setDatasetDigest(bytes(session.datasetDigest()))
        .setCredentialGeneration(session.credentialGeneration())
        .setAuthorizationGeneration(session.authorizationGeneration())
        .setCreatedAtEpochMillis(session.createdAtEpochMillis())
        .setExpiresAtEpochMillis(session.expiresAtEpochMillis())
        .setSlotIndex(session.slotIndex())
        .setSlotGeneration(session.slotGeneration())
        .setStatus(sessionStatus(session.status()))
        .setCredentialEnvelope(envelope)
        .build();
    return checkedSize(encoded.toByteArray());
  }

  public static AuthSession decodeSession(byte[] bytes) {
    AuthProtos.AuthSessionRecord record = parse(bytes, AuthProtos.AuthSessionRecord::parseFrom);
    if (!record.getUnknownFields().asMap().isEmpty()
        || !record.hasFormatVersion() || record.getFormatVersion() != FORMAT_VERSION
        || !record.hasSessionId() || !record.hasTokenDigest() || !record.hasSubjectDigest()
        || !record.hasTrustDomain() || !record.hasProviderId() || !record.hasTableGroupId()
        || !record.hasConnectionProfileId() || !record.hasDatasetDigest()
        || !record.hasCredentialGeneration() || !record.hasAuthorizationGeneration()
        || !record.hasCreatedAtEpochMillis() || !record.hasExpiresAtEpochMillis()
        || !record.hasSlotIndex() || !record.hasSlotGeneration() || !record.hasStatus()
        || !record.hasCredentialEnvelope()) {
      throw invalid();
    }
    AuthProtos.CredentialEnvelope envelope = record.getCredentialEnvelope();
    if (!envelope.getUnknownFields().asMap().isEmpty()
        || !envelope.hasKeyId() || !envelope.hasAadVersion() || !envelope.hasNonce()
        || !envelope.hasCiphertext()) {
      throw invalid();
    }
    try {
      return new AuthSession(
          key(record.getSessionId()), key(record.getTokenDigest()), key(record.getSubjectDigest()),
          record.getTrustDomain(), record.getProviderId(),
          new AuthenticationScope(record.getTableGroupId(), record.getConnectionProfileId()),
          key(record.getDatasetDigest()), record.getCredentialGeneration(),
          record.getAuthorizationGeneration(), record.getCreatedAtEpochMillis(),
          record.getExpiresAtEpochMillis(), record.getSlotIndex(), record.getSlotGeneration(),
          sessionStatus(record.getStatus()),
          new CredentialEnvelope(envelope.getKeyId(), envelope.getAadVersion(),
              envelope.getNonce().toByteArray(), envelope.getCiphertext().toByteArray()));
    } catch (IllegalArgumentException | NullPointerException invalidRecord) {
      throw invalid();
    }
  }

  public static byte[] encode(AuthSlot slot) {
    Objects.requireNonNull(slot, "slot");
    var builder = AuthProtos.AuthSlotRecord.newBuilder()
        .setFormatVersion(FORMAT_VERSION).setIndex(slot.index())
        .setGeneration(slot.generation()).setStatus(slotStatus(slot.state()));
    if (slot.sessionId() != null) builder.setSessionId(bytes(slot.sessionId()));
    if (slot.operationId() != null) builder.setOperationId(slot.operationId());
    return checkedSize(builder.build().toByteArray());
  }

  public static AuthSlot decodeSlot(byte[] bytes) {
    AuthProtos.AuthSlotRecord record = parse(bytes, AuthProtos.AuthSlotRecord::parseFrom);
    if (!record.getUnknownFields().asMap().isEmpty()
        || !record.hasFormatVersion() || record.getFormatVersion() != FORMAT_VERSION
        || !record.hasIndex() || !record.hasGeneration() || !record.hasStatus()) {
      throw invalid();
    }
    AuthSlot.State state = slotStatus(record.getStatus());
    try {
      ByteKey sessionId = record.hasSessionId() ? key(record.getSessionId()) : null;
      String operationId = record.hasOperationId() ? record.getOperationId() : null;
      return new AuthSlot(record.getIndex(), record.getGeneration(), sessionId, operationId, state);
    } catch (IllegalArgumentException | NullPointerException invalidRecord) {
      throw invalid();
    }
  }

  public static byte[] encode(SubjectAuthority authority) {
    Objects.requireNonNull(authority, "authority");
    var encoded = AuthProtos.SubjectAuthorityRecord.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setSubjectDigest(bytes(authority.subjectDigest()))
        .setTrustDomain(authority.trustDomain())
        .setProviderId(authority.providerId())
        .setTableGroupId(authority.scope().tableGroupId())
        .setConnectionProfileId(authority.scope().connectionProfileId())
        .setCredentialGeneration(authority.credentialGeneration())
        .setAuthorizationGeneration(authority.authorizationGeneration())
        .setStatus(authorityStatus(authority.state()))
        .build();
    return checkedSize(encoded.toByteArray());
  }

  public static SubjectAuthority decodeAuthority(byte[] bytes) {
    AuthProtos.SubjectAuthorityRecord record = parse(bytes, AuthProtos.SubjectAuthorityRecord::parseFrom);
    if (!record.getUnknownFields().asMap().isEmpty()
        || !record.hasFormatVersion() || record.getFormatVersion() != FORMAT_VERSION
        || !record.hasSubjectDigest() || !record.hasTrustDomain() || !record.hasProviderId()
        || !record.hasTableGroupId() || !record.hasConnectionProfileId()
        || !record.hasCredentialGeneration() || !record.hasAuthorizationGeneration()
        || !record.hasStatus()) {
      throw invalid();
    }
    try {
      return new SubjectAuthority(key(record.getSubjectDigest()), record.getTrustDomain(),
          record.getProviderId(),
          new AuthenticationScope(record.getTableGroupId(), record.getConnectionProfileId()),
          record.getCredentialGeneration(), record.getAuthorizationGeneration(),
          authorityStatus(record.getStatus()));
    } catch (IllegalArgumentException | NullPointerException invalidRecord) {
      throw invalid();
    }
  }

  private static AuthProtos.AuthSessionStatus sessionStatus(AuthSession.Status status) {
    return switch (status) {
      case PREPARING -> AuthProtos.AuthSessionStatus.AUTH_SESSION_PREPARING;
      case ACTIVE -> AuthProtos.AuthSessionStatus.AUTH_SESSION_ACTIVE;
      case RETIRING -> AuthProtos.AuthSessionStatus.AUTH_SESSION_RETIRING;
      case REVOKED -> AuthProtos.AuthSessionStatus.AUTH_SESSION_REVOKED;
      case EXPIRED -> AuthProtos.AuthSessionStatus.AUTH_SESSION_EXPIRED;
    };
  }

  private static AuthSession.Status sessionStatus(AuthProtos.AuthSessionStatus status) {
    return switch (status) {
      case AUTH_SESSION_PREPARING -> AuthSession.Status.PREPARING;
      case AUTH_SESSION_ACTIVE -> AuthSession.Status.ACTIVE;
      case AUTH_SESSION_RETIRING -> AuthSession.Status.RETIRING;
      case AUTH_SESSION_REVOKED -> AuthSession.Status.REVOKED;
      case AUTH_SESSION_EXPIRED -> AuthSession.Status.EXPIRED;
      default -> throw invalid();
    };
  }

  private static AuthProtos.AuthSlotStatus slotStatus(AuthSlot.State status) {
    return switch (status) {
      case EMPTY -> AuthProtos.AuthSlotStatus.AUTH_SLOT_EMPTY;
      case RESERVED -> AuthProtos.AuthSlotStatus.AUTH_SLOT_RESERVED;
      case ACTIVE -> AuthProtos.AuthSlotStatus.AUTH_SLOT_ACTIVE;
      case RETIRING -> AuthProtos.AuthSlotStatus.AUTH_SLOT_RETIRING;
    };
  }

  private static AuthSlot.State slotStatus(AuthProtos.AuthSlotStatus status) {
    return switch (status) {
      case AUTH_SLOT_EMPTY -> AuthSlot.State.EMPTY;
      case AUTH_SLOT_RESERVED -> AuthSlot.State.RESERVED;
      case AUTH_SLOT_ACTIVE -> AuthSlot.State.ACTIVE;
      case AUTH_SLOT_RETIRING -> AuthSlot.State.RETIRING;
      default -> throw invalid();
    };
  }

  private static AuthProtos.SubjectAuthorityStatus authorityStatus(SubjectAuthority.State status) {
    return switch (status) {
      case ACTIVE -> AuthProtos.SubjectAuthorityStatus.SUBJECT_AUTHORITY_ACTIVE;
      case REVOKED -> AuthProtos.SubjectAuthorityStatus.SUBJECT_AUTHORITY_REVOKED;
    };
  }

  private static SubjectAuthority.State authorityStatus(
      AuthProtos.SubjectAuthorityStatus status) {
    return switch (status) {
      case SUBJECT_AUTHORITY_ACTIVE -> SubjectAuthority.State.ACTIVE;
      case SUBJECT_AUTHORITY_REVOKED -> SubjectAuthority.State.REVOKED;
      default -> throw invalid();
    };
  }

  private static ByteString bytes(ByteKey value) {
    return ByteString.copyFrom(value.bytes());
  }

  private static ByteKey key(ByteString value) {
    return ByteKey.copyOf(value.toByteArray());
  }

  private static byte[] checkedSize(byte[] encoded) {
    if (encoded.length > MAX_RECORD_BYTES) throw invalid();
    return encoded;
  }

  private static <T> T parse(byte[] encoded,
      CheckedParser<T> parser) {
    Objects.requireNonNull(encoded, "encoded");
    if (encoded.length == 0 || encoded.length > MAX_RECORD_BYTES) throw invalid();
    try {
      return parser.parse(encoded);
    } catch (InvalidProtocolBufferException malformed) {
      throw new IllegalArgumentException("invalid auth-control record", malformed);
    }
  }

  @FunctionalInterface
  private interface CheckedParser<T> {
    T parse(byte[] encoded) throws InvalidProtocolBufferException;
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("auth-control record violates its format");
  }
}
