package io.etcdhbase.hbase.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.etcdhbase.core.auth.AuthDeploymentRegistry;
import io.etcdhbase.core.auth.AuthConfigurationUpdate;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.hbase.auth.proto.AuthProtos;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/** Canonical checked encoding of the deployment inventory and its operation. */
public final class AuthDeploymentRegistryMessages {
  private static final int VERSION = 1;

  private AuthDeploymentRegistryMessages() {}

  public static byte[] encode(AuthDeploymentRegistry registry) {
    Objects.requireNonNull(registry, "registry");
    var record = AuthProtos.AuthDeploymentRegistryRecord.newBuilder()
        .setFormatVersion(VERSION).setTrustDomain(registry.trustDomain())
        .setConfigurationGeneration(registry.configurationGeneration()).setRevision(registry.revision());
    for (var group : registry.groups()) {
      record.addGroups(encodeGroup(group));
    }
    if (registry.revocation() != null) {
      record.setRevocation(ByteString.copyFrom(AuthorizationRevocationMessages.encode(registry.revocation())));
    }
    if (registry.configurationUpdate() != null) {
      var update = registry.configurationUpdate();
      var encoded = AuthProtos.AuthConfigurationUpdate.newBuilder()
          .setOperationSequence(update.operationSequence());
      update.replacement().forEach(group -> encoded.addReplacement(encodeGroup(group)));
      update.targets().forEach(target -> encoded.addTargets(AuthProtos.AuthConfigurationBarrierTarget.newBuilder()
          .setTableGroupId(target.tableGroupId()).setDatasetDigest(ByteString.copyFrom(target.datasetDigest().bytes()))
          .setBarrierGeneration(target.barrierGeneration()).setInstalled(target.installed())));
      record.setConfigurationUpdate(encoded);
    }
    byte[] content = record.build().toByteArray();
    byte[] result = AuthProtos.AuthDeploymentRegistryEnvelope.newBuilder()
        .setContent(ByteString.copyFrom(content)).setSha256(ByteString.copyFrom(sha256(content)))
        .build().toByteArray();
    if (result.length > AuthMessages.MAX_RECORD_BYTES) throw invalid();
    return result;
  }

  public static AuthDeploymentRegistry decode(byte[] encoded) {
    if (encoded == null || encoded.length == 0 || encoded.length > AuthMessages.MAX_RECORD_BYTES) {
      throw invalid();
    }
    try {
      var envelope = AuthProtos.AuthDeploymentRegistryEnvelope.parseFrom(encoded);
      if (!envelope.getUnknownFields().asMap().isEmpty() || !envelope.hasContent()
          || !envelope.hasSha256() || !Arrays.equals(encoded, envelope.toByteArray())) throw invalid();
      byte[] content = envelope.getContent().toByteArray();
      if (!MessageDigest.isEqual(sha256(content), envelope.getSha256().toByteArray())) throw invalid();
      var record = AuthProtos.AuthDeploymentRegistryRecord.parseFrom(content);
      if (!record.getUnknownFields().asMap().isEmpty() || !Arrays.equals(content, record.toByteArray())
          || !record.hasFormatVersion() || record.getFormatVersion() != VERSION
          || !record.hasTrustDomain() || !record.hasConfigurationGeneration() || !record.hasRevision()) {
        throw invalid();
      }
      var groups = record.getGroupsList().stream().map(AuthDeploymentRegistryMessages::decodeGroup).toList();
      return new AuthDeploymentRegistry(record.getTrustDomain(), record.getConfigurationGeneration(),
          record.getRevision(), groups, record.hasRevocation()
              ? AuthorizationRevocationMessages.decode(record.getRevocation().toByteArray()) : null,
          record.hasConfigurationUpdate() ? decodeUpdate(record.getConfigurationUpdate()) : null);
    } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
      throw invalid();
    }
  }

  private static AuthProtos.AuthDeploymentGroup encodeGroup(AuthDeploymentRegistry.Group group) {
    return AuthProtos.AuthDeploymentGroup.newBuilder()
        .setTableGroupId(group.tableGroupId()).setProviderId(group.providerId())
        .setBackendIdentityDomain(group.backendIdentityDomain()).setConnectionProfileId(group.connectionProfileId())
        .setDatasetDigest(ByteString.copyFrom(group.datasetDigest().bytes()))
        .setConfigurationDigest(ByteString.copyFrom(group.configurationDigest().bytes()))
        .setEnabled(group.enabled()).build();
  }

  private static AuthDeploymentRegistry.Group decodeGroup(AuthProtos.AuthDeploymentGroup group) {
    if (!group.getUnknownFields().asMap().isEmpty() || !group.hasTableGroupId()
        || !group.hasProviderId() || !group.hasConnectionProfileId() || !group.hasDatasetDigest()
        || !group.hasConfigurationDigest() || !group.hasEnabled() || !group.hasBackendIdentityDomain()) {
      throw invalid();
    }
    return new AuthDeploymentRegistry.Group(group.getTableGroupId(), group.getProviderId(),
        group.getBackendIdentityDomain(), group.getConnectionProfileId(),
        ByteKey.copyOf(group.getDatasetDigest().toByteArray()),
        ByteKey.copyOf(group.getConfigurationDigest().toByteArray()), group.getEnabled());
  }

  private static AuthConfigurationUpdate decodeUpdate(AuthProtos.AuthConfigurationUpdate update) {
    if (!update.getUnknownFields().asMap().isEmpty() || !update.hasOperationSequence()) throw invalid();
    var targets = update.getTargetsList().stream().map(target -> {
      if (!target.getUnknownFields().asMap().isEmpty() || !target.hasTableGroupId()
          || !target.hasDatasetDigest() || !target.hasBarrierGeneration() || !target.hasInstalled()) throw invalid();
      return new AuthConfigurationUpdate.Target(target.getTableGroupId(),
          ByteKey.copyOf(target.getDatasetDigest().toByteArray()), target.getBarrierGeneration(), target.getInstalled());
    }).toList();
    return new AuthConfigurationUpdate(update.getOperationSequence(),
        update.getReplacementList().stream().map(AuthDeploymentRegistryMessages::decodeGroup).toList(), targets);
  }

  private static byte[] sha256(byte[] value) {
    try { return MessageDigest.getInstance("SHA-256").digest(value); }
    catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("auth deployment registry violates its format");
  }
}
