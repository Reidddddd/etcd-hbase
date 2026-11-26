package io.etcdhbase.server.auth;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.server.config.M3AuthenticationConfiguration;
import io.etcdhbase.spi.identity.AuthenticationScope;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Objects;

/** Deterministic identity of the effective bridge configuration; no configuration is persisted. */
public final class AuthConfigurationFingerprint {
  private AuthConfigurationFingerprint() {}

  public static ByteKey compute(M3AuthenticationConfiguration configuration,
      AuthenticationScope scope, ByteKey datasetDigest) {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(scope, "scope");
    if (datasetDigest == null || datasetDigest.length() != 32) {
      throw new IllegalArgumentException("invalid dataset digest");
    }
    try {
      var mapper = JsonMapper.builder().enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS).build();
      byte[] configurationBytes = mapper.writeValueAsBytes(canonicalize(configuration));
      var bytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(bytes)) {
        output.writeUTF("etcd-hbase/auth-configuration/v1");
        output.writeUTF(scope.tableGroupId());
        output.writeUTF(scope.connectionProfileId());
        output.write(datasetDigest.bytes());
        output.writeInt(configurationBytes.length);
        output.write(configurationBytes);
      }
      return ByteKey.copyOf(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (IOException | NoSuchAlgorithmException failure) {
      throw new IllegalStateException("cannot fingerprint authentication configuration", failure);
    }
  }

  private static M3AuthenticationConfiguration canonicalize(M3AuthenticationConfiguration input) {
    var clients = input.clientProfiles().stream().map(client ->
        new M3AuthenticationConfiguration.ClientProfile(client.id(), client.hbaseProfileId(),
            client.authenticatorId(), client.clusterId(), client.hbaseSiteFile(),
            client.credentialVersions().stream()
                .sorted(Comparator.comparing(M3AuthenticationConfiguration.CredentialVersion::principal))
                .toList(), client.backendIdentityDomain()))
        .sorted(Comparator.comparing(M3AuthenticationConfiguration.ClientProfile::id)).toList();
    var endpoints = input.endpointBindings().stream()
        .sorted(Comparator.comparing(M3AuthenticationConfiguration.EndpointBinding::tableGroupId))
        .toList();
    var backgrounds = input.backgroundIdentities().stream().map(identity ->
        new M3AuthenticationConfiguration.BackgroundIdentity(identity.tableGroupId(),
            identity.providerId(), identity.clientProfileId(), identity.identity(),
            identity.allowedTableGroupIds().stream().sorted().toList()))
        .sorted(Comparator.comparing(M3AuthenticationConfiguration.BackgroundIdentity::tableGroupId))
        .toList();
    var keys = input.credentialKeyRing().keys().stream()
        .sorted(Comparator.comparing(M3AuthenticationConfiguration.KeyReference::keyId)).toList();
    var keyRing = new M3AuthenticationConfiguration.CredentialKeyRing(
        input.credentialKeyRing().activeKeyId(), keys);
    return new M3AuthenticationConfiguration(input.trustDomain(), input.sessionTtlMillis(),
        input.sessionSlots(), input.maximumIdentityContainers(), input.secretDirectory(),
        input.authControl(), clients, endpoints, backgrounds, keyRing);
  }
}
