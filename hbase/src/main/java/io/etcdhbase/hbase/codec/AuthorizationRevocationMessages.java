package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.auth.AuthorizationRevocation;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.auth.proto.AuthProtos;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/** Canonical, checksummed storage format for the frozen revocation snapshot and progress. */
public final class AuthorizationRevocationMessages {
  private static final int VERSION = 1;
  private static final int MAX_BYTES = 128 * 1024;

  private AuthorizationRevocationMessages() {}

  public static byte[] encode(AuthorizationRevocation operation) {
    Objects.requireNonNull(operation, "operation");
    var record = AuthProtos.AuthorizationRevocationRecord.newBuilder()
        .setFormatVersion(VERSION).setOperationId(bytes(operation.operationId()))
        .setSubjectFamilyDigest(bytes(operation.subjectFamilyDigest()))
        .setProviderId(operation.providerId()).setBackendIdentityDomain(operation.backendIdentityDomain())
        .setCredentialGeneration(operation.credentialGeneration())
        .setOperationSequence(operation.operationSequence())
        .setConfigurationGeneration(operation.configurationGeneration())
        .setRevision(operation.revision()).setState(state(operation.state()));
    for (var target : operation.targets()) {
      var identity = target.identity();
      record.addTargets(AuthProtos.AuthorizationRevocationTarget.newBuilder()
          .setTableGroupId(identity.tableGroupId()).setDatasetDigest(bytes(identity.datasetDigest()))
          .setAuthorityDigest(bytes(identity.authorityDigest()))
          .setBarrierGeneration(target.barrierGeneration()).setInstalled(target.installed())
          .setReleased(target.released()));
    }
    byte[] content = record.build().toByteArray();
    byte[] encoded = AuthProtos.AuthorizationRevocationEnvelope.newBuilder()
        .setContent(ByteString.copyFrom(content)).setSha256(ByteString.copyFrom(sha256(content)))
        .build().toByteArray();
    if (encoded.length > MAX_BYTES) throw invalid();
    return encoded;
  }

  public static AuthorizationRevocation decode(byte[] encoded) {
    if (encoded == null || encoded.length == 0 || encoded.length > MAX_BYTES) throw invalid();
    try {
      var envelope = AuthProtos.AuthorizationRevocationEnvelope.parseFrom(encoded);
      if (!envelope.getUnknownFields().asMap().isEmpty() || !envelope.hasContent()
          || !envelope.hasSha256() || !Arrays.equals(encoded, envelope.toByteArray())) throw invalid();
      byte[] content = envelope.getContent().toByteArray();
      if (!MessageDigest.isEqual(sha256(content), envelope.getSha256().toByteArray())) throw invalid();
      var record = AuthProtos.AuthorizationRevocationRecord.parseFrom(content);
      if (!record.getUnknownFields().asMap().isEmpty() || !Arrays.equals(content, record.toByteArray())
          || !record.hasFormatVersion() || record.getFormatVersion() != VERSION
          || !record.hasOperationId() || !record.hasSubjectFamilyDigest()
          || !record.hasConfigurationGeneration() || !record.hasRevision() || !record.hasState()
          || !record.hasProviderId() || !record.hasBackendIdentityDomain()
          || !record.hasCredentialGeneration() || !record.hasOperationSequence()) {
        throw invalid();
      }
      var targets = record.getTargetsList().stream().map(target -> {
        if (!target.getUnknownFields().asMap().isEmpty() || !target.hasTableGroupId()
            || !target.hasDatasetDigest() || !target.hasAuthorityDigest()
            || !target.hasBarrierGeneration() || !target.hasInstalled() || !target.hasReleased()) throw invalid();
        return new AuthorizationRevocation.Target(new AuthorizationRevocation.TargetIdentity(
            target.getTableGroupId(), key(target.getDatasetDigest()), key(target.getAuthorityDigest())),
            target.getBarrierGeneration(), target.getInstalled(), target.getReleased());
      }).toList();
      return new AuthorizationRevocation(key(record.getOperationId()), key(record.getSubjectFamilyDigest()),
          record.getProviderId(), record.getBackendIdentityDomain(), record.getCredentialGeneration(),
          record.getOperationSequence(),
          record.getConfigurationGeneration(), record.getRevision(), state(record.getState()), targets);
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw invalid();
    }
  }

  private static int state(AuthorizationRevocation.State state) {
    return switch (state) { case INSTALLING -> 1; case COMPLETED -> 2; };
  }

  private static AuthorizationRevocation.State state(int state) {
    return switch (state) {
      case 1 -> AuthorizationRevocation.State.INSTALLING;
      case 2 -> AuthorizationRevocation.State.COMPLETED;
      default -> throw invalid();
    };
  }

  private static ByteString bytes(ByteKey key) { return ByteString.copyFrom(key.bytes()); }
  private static ByteKey key(ByteString bytes) { return ByteKey.copyOf(bytes.toByteArray()); }

  private static byte[] sha256(byte[] bytes) {
    try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
    catch (NoSuchAlgorithmException failure) { throw new IllegalStateException(failure); }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("authorization revocation record violates its format");
  }
}
