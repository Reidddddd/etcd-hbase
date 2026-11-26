package io.etcdhbase.server.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.etcdhbase.admin.config.InitializationConfiguration;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Explicit M5 identity, auth-control and credential-reference bindings for an M3 runtime. */
public record M3AuthenticationConfiguration(
    String trustDomain,
    long sessionTtlMillis,
    int sessionSlots,
    int maximumIdentityContainers,
    @JsonProperty(required = false) String secretDirectory,
    AuthControl authControl,
    List<ClientProfile> clientProfiles,
    List<EndpointBinding> endpointBindings,
    List<BackgroundIdentity> backgroundIdentities,
    CredentialKeyRing credentialKeyRing) {

  public M3AuthenticationConfiguration {
    trustDomain = requireIdentifier(trustDomain, "trustDomain");
    if (sessionTtlMillis < 1_000 || sessionTtlMillis > 30L * 24 * 60 * 60 * 1_000
        || sessionSlots < 1 || sessionSlots > 1_024
        || maximumIdentityContainers < 1 || maximumIdentityContainers > 1_024) {
      throw new IllegalArgumentException("M5 authentication limits are outside their bounds");
    }
    if (secretDirectory != null) {
      Path directory = Path.of(secretDirectory);
      if (!directory.isAbsolute()) {
        throw new IllegalArgumentException("secret directory must be an absolute path");
      }
      secretDirectory = directory.normalize().toString();
    }
    Objects.requireNonNull(authControl, "authControl");
    clientProfiles = boundedCopy(clientProfiles, 32, "clientProfiles");
    endpointBindings = boundedCopy(endpointBindings, 32, "endpointBindings");
    backgroundIdentities = boundedCopy(backgroundIdentities, 32, "backgroundIdentities");
    Objects.requireNonNull(credentialKeyRing, "credentialKeyRing");
    requireUnique(clientProfiles.stream().map(ClientProfile::id).toList(), "client profile IDs");
    requireUnique(endpointBindings.stream().map(EndpointBinding::tableGroupId).toList(),
        "endpoint table-group bindings");
    requireUnique(backgroundIdentities.stream().map(BackgroundIdentity::tableGroupId).toList(),
        "background identity table-group bindings");
    if (endpointBindings.isEmpty() || backgroundIdentities.isEmpty()) {
      throw new IllegalArgumentException("M5 requires endpoint and background identity bindings");
    }
    if (clientProfiles.stream().noneMatch(profile -> profile.id().equals(authControl.clientProfileId()))) {
      throw new IllegalArgumentException("auth-control client profile is not configured");
    }
    for (EndpointBinding binding : endpointBindings) {
      requireConfiguredProfile(binding.clientProfileId(), clientProfiles);
    }
    for (BackgroundIdentity identity : backgroundIdentities) {
      requireConfiguredProfile(identity.clientProfileId(), clientProfiles);
    }
    requireSecretDirectoryForReferences(secretDirectory, authControl.identity().credentialReference(),
        credentialKeyRing.keys().stream().map(KeyReference::secretReference).toList(),
        backgroundIdentities.stream().map(value -> value.identity().credentialReference()).toList());
    requireSecretDirectoryForReferences(secretDirectory, clientProfiles.stream()
        .flatMap(profile -> profile.credentialVersions().stream())
        .map(CredentialVersion::credentialReference).toArray(String[]::new));
  }

  /** Unique auth-control storage; the administrator used for fresh table creation is separate. */
  public record AuthControl(
      String hbaseProfileId,
      String saltProfileId,
      String tableName,
      String providerId,
      String clientProfileId,
      IdentityCredential identity) {
    public AuthControl {
      hbaseProfileId = requireIdentifier(hbaseProfileId, "authControl.hbaseProfileId");
      saltProfileId = requireIdentifier(saltProfileId, "authControl.saltProfileId");
      if (tableName == null || !tableName.matches("[A-Za-z_][A-Za-z0-9_.-]{0,127}")) {
        throw new IllegalArgumentException("auth-control table name is invalid");
      }
      providerId = requireIdentifier(providerId, "authControl.providerId");
      clientProfileId = requireIdentifier(clientProfileId, "authControl.clientProfileId");
      Objects.requireNonNull(identity, "identity");
    }
  }

  /** Client security for one explicit backend profile and authenticator adapter. */
  public record ClientProfile(
      String id,
      String hbaseProfileId,
      String authenticatorId,
      @JsonProperty(required = false) String clusterId,
      String hbaseSiteFile,
      @JsonProperty(required = false) List<CredentialVersion> credentialVersions,
      @JsonProperty(required = false) String backendIdentityDomain) {
    public ClientProfile(String id, String hbaseProfileId, String authenticatorId,
        String clusterId, String hbaseSiteFile) {
      this(id, hbaseProfileId, authenticatorId, clusterId, hbaseSiteFile, List.of(), null);
    }

    public ClientProfile(String id, String hbaseProfileId, String authenticatorId,
        String clusterId, String hbaseSiteFile, List<CredentialVersion> credentialVersions) {
      this(id, hbaseProfileId, authenticatorId, clusterId, hbaseSiteFile, credentialVersions, null);
    }

    public ClientProfile {
      id = requireIdentifier(id, "client profile ID");
      hbaseProfileId = requireIdentifier(hbaseProfileId, "clientProfile.hbaseProfileId");
      authenticatorId = requireIdentifier(authenticatorId, "clientProfile.authenticatorId");
      if (clusterId != null && (clusterId.isBlank() || clusterId.length() > 512
          || clusterId.codePoints().anyMatch(Character::isISOControl))) {
        throw new IllegalArgumentException("backend cluster ID is invalid");
      }
      if (hbaseSiteFile == null || !Path.of(hbaseSiteFile).isAbsolute()) {
        throw new IllegalArgumentException("secure HBase client configuration must be an absolute file path");
      }
      hbaseSiteFile = Path.of(hbaseSiteFile).normalize().toString();
      credentialVersions = credentialVersions == null ? List.of() : List.copyOf(credentialVersions);
      if (credentialVersions.size() > 1_024) {
        throw new IllegalArgumentException("credential version bindings exceed their limit");
      }
      requireUnique(credentialVersions.stream().map(CredentialVersion::principal).toList(),
          "credential version principals");
      if (backendIdentityDomain != null) {
        backendIdentityDomain = requireIdentifier(backendIdentityDomain, "clientProfile.backendIdentityDomain");
      }
    }
  }

  /** Operator-managed binding of an exact credential to its monotonically increasing generation. */
  public record CredentialVersion(String principal, long generation, String credentialReference) {
    public CredentialVersion {
      if (principal == null || principal.isBlank() || principal.length() > 256
          || principal.codePoints().anyMatch(Character::isISOControl) || generation < 1) {
        throw new IllegalArgumentException("credential version binding is invalid");
      }
      credentialReference = requireCredentialReference(credentialReference);
    }
  }

  /** Endpoint identity selection is fixed by the listener's table-group binding. */
  public record EndpointBinding(String tableGroupId, String providerId, String clientProfileId) {
    public EndpointBinding {
      tableGroupId = requireIdentifier(tableGroupId, "endpointBinding.tableGroupId");
      providerId = requireIdentifier(providerId, "endpointBinding.providerId");
      clientProfileId = requireIdentifier(clientProfileId, "endpointBinding.clientProfileId");
    }
  }

  /** Server-held identity used only for recovery and background work for the listed groups. */
  public record BackgroundIdentity(
      String tableGroupId,
      String providerId,
      String clientProfileId,
      IdentityCredential identity,
      List<String> allowedTableGroupIds) {
    public BackgroundIdentity {
      tableGroupId = requireIdentifier(tableGroupId, "backgroundIdentity.tableGroupId");
      providerId = requireIdentifier(providerId, "backgroundIdentity.providerId");
      clientProfileId = requireIdentifier(clientProfileId, "backgroundIdentity.clientProfileId");
      Objects.requireNonNull(identity, "identity");
      allowedTableGroupIds = boundedCopy(allowedTableGroupIds, 32, "allowedTableGroupIds");
      if (allowedTableGroupIds.isEmpty() || !allowedTableGroupIds.contains(tableGroupId)) {
        throw new IllegalArgumentException("background identity must explicitly allow its table group");
      }
      requireUnique(allowedTableGroupIds, "background allowed table groups");
    }
  }

  /** Credential reference or trusted server-side Kerberos source; never client-selected. */
  public record IdentityCredential(
      CredentialSource source,
      @JsonProperty(required = false) String principal,
      String credentialReference) {
    public IdentityCredential {
      Objects.requireNonNull(source, "identityCredential.source");
      credentialReference = requireCredentialReference(credentialReference);
      if (source == CredentialSource.CREDENTIAL_REFERENCE) {
        if (principal != null) {
          throw new IllegalArgumentException("credential-reference identity gets its principal from the secret");
        }
      } else {
        requireKerberosPrincipal(principal);
        if (!credentialReference.startsWith("secret://")) {
          throw new IllegalArgumentException("trusted Kerberos material must use a mounted secret reference");
        }
      }
    }
  }

  public enum CredentialSource { CREDENTIAL_REFERENCE, KERBEROS_KEYTAB, KERBEROS_TICKET_CACHE }

  /** Versioned key references; every key remains outside HBase and the application config. */
  public record CredentialKeyRing(String activeKeyId, List<KeyReference> keys) {
    public CredentialKeyRing {
      activeKeyId = requireIdentifier(activeKeyId, "active credential key ID");
      keys = boundedCopy(keys, 16, "credential keys");
      String configuredActiveKeyId = activeKeyId;
      if (keys.isEmpty() || keys.stream().noneMatch(key -> key.keyId().equals(configuredActiveKeyId))) {
        throw new IllegalArgumentException("active credential key is not configured");
      }
      requireUnique(keys.stream().map(KeyReference::keyId).toList(), "credential key IDs");
    }
  }

  public record KeyReference(String keyId, String secretReference) {
    public KeyReference {
      keyId = requireIdentifier(keyId, "credential key ID");
      secretReference = requireCredentialReference(secretReference);
      if (!secretReference.startsWith("secret://")) {
        throw new IllegalArgumentException("credential encryption keys must use mounted secret references");
      }
    }
  }

  public static void validateAgainst(
      M3AuthenticationConfiguration authentication,
      RuntimeConfiguration configuration) {
    Objects.requireNonNull(authentication, "authentication");
    Objects.requireNonNull(configuration, "configuration");
    InitializationConfiguration initialization = configuration.initialization();
    var groups = initialization.tableGroups().stream().collect(
        java.util.stream.Collectors.toMap(InitializationConfiguration.TableGroup::id, group -> group));
    var hbaseProfiles = initialization.hbaseProfiles().stream().collect(
        java.util.stream.Collectors.toMap(InitializationConfiguration.HBaseProfile::id, profile -> profile));
    var saltProfiles = initialization.saltProfiles().stream().collect(
        java.util.stream.Collectors.toMap(InitializationConfiguration.SaltProfile::id, profile -> profile));
    var clients = authentication.clientProfiles().stream().collect(
        java.util.stream.Collectors.toMap(ClientProfile::id, profile -> profile));
    var listenersByGroup = configuration.listeners().stream().collect(
        java.util.stream.Collectors.groupingBy(RuntimeConfiguration.Listener::tableGroupId));

    validateSecureProfile(authentication.authControl().hbaseProfileId(), hbaseProfiles, clients,
        authentication.authControl().clientProfileId());
    if (!saltProfiles.containsKey(authentication.authControl().saltProfileId())) {
      throw new IllegalArgumentException("auth-control salt profile is not configured");
    }
    for (EndpointBinding endpoint : authentication.endpointBindings()) {
      var group = groups.get(endpoint.tableGroupId());
      if (group == null || !group.identityProviderId().equals(endpoint.providerId())) {
        throw new IllegalArgumentException("endpoint auth binding does not match its table group");
      }
      validateSecureProfile(group.hbaseProfileId(), hbaseProfiles, clients, endpoint.clientProfileId());
      if (listenersByGroup.getOrDefault(endpoint.tableGroupId(), List.of()).stream()
          .anyMatch(listener -> listener.tls() == null)) {
        throw new IllegalArgumentException("credential-reference listeners require TLS");
      }
    }
    for (BackgroundIdentity identity : authentication.backgroundIdentities()) {
      var group = groups.get(identity.tableGroupId());
      if (group == null || !group.identityProviderId().equals(identity.providerId())) {
        throw new IllegalArgumentException("background identity does not match its table group");
      }
      validateSecureProfile(group.hbaseProfileId(), hbaseProfiles, clients, identity.clientProfileId());
      if (!groups.keySet().containsAll(identity.allowedTableGroupIds())) {
        throw new IllegalArgumentException("background identity allows an unknown table group");
      }
    }
    for (var listener : configuration.listeners()) {
      var group = groups.get(listener.tableGroupId());
      var profile = hbaseProfiles.get(group.hbaseProfileId());
      if (profile.authenticationMode() == InitializationConfiguration.AuthenticationMode.CREDENTIAL_REFERENCE
          && authentication.endpointBindings().stream().noneMatch(binding ->
              binding.tableGroupId().equals(group.id()))) {
        throw new IllegalArgumentException("credential-reference endpoint has no identity binding");
      }
      if (profile.authenticationMode() == InitializationConfiguration.AuthenticationMode.DEV_NO_AUTH
          && authentication.endpointBindings().stream().anyMatch(binding ->
              binding.tableGroupId().equals(group.id()))) {
        throw new IllegalArgumentException("development endpoint cannot use credential-reference auth");
      }
    }
    if (authentication.endpointBindings().stream().anyMatch(binding ->
        !listenersByGroup.containsKey(binding.tableGroupId()))) {
      throw new IllegalArgumentException("endpoint auth binding references an inactive table group");
    }
    if (authentication.backgroundIdentities().stream().anyMatch(identity ->
        !listenersByGroup.containsKey(identity.tableGroupId()))) {
      throw new IllegalArgumentException("background identity references an inactive table group");
    }
    if (authentication.authControl().identity().source() == CredentialSource.CREDENTIAL_REFERENCE
        && hbaseProfiles.get(authentication.authControl().hbaseProfileId()).authenticationMode()
            == InitializationConfiguration.AuthenticationMode.DEV_NO_AUTH) {
      throw new IllegalArgumentException("auth-control service identity cannot use development authentication");
    }
  }

  private static void validateSecureProfile(String profileId,
      java.util.Map<String, InitializationConfiguration.HBaseProfile> hbaseProfiles,
      java.util.Map<String, ClientProfile> clients, String clientProfileId) {
    var hbaseProfile = hbaseProfiles.get(profileId);
    var clientProfile = clients.get(clientProfileId);
    if (hbaseProfile == null || hbaseProfile.authenticationMode()
        != InitializationConfiguration.AuthenticationMode.CREDENTIAL_REFERENCE
        || clientProfile == null || !clientProfile.hbaseProfileId().equals(profileId)) {
      throw new IllegalArgumentException("M5 client security profile does not match its HBase profile");
    }
    if ((clientProfile.authenticatorId().equals("delegation-token")
        || clientProfile.authenticatorId().equals("digest-password"))
        && (clientProfile.clusterId() == null || clientProfile.clusterId().isBlank())) {
      throw new IllegalArgumentException("selected HBase authenticator requires the actual cluster ID");
    }
  }

  private static void requireSecretDirectoryForReferences(String directory, String... references) {
    if (directory == null && java.util.Arrays.stream(references)
        .anyMatch(reference -> reference != null && reference.startsWith("secret://"))) {
      throw new IllegalArgumentException("mounted secret references require secret_directory");
    }
  }

  private static void requireSecretDirectoryForReferences(String directory,
      String authControlReference, List<String> keyReferences, List<String> backgroundReferences) {
    if (directory == null && java.util.stream.Stream.concat(
        java.util.stream.Stream.of(authControlReference),
        java.util.stream.Stream.concat(keyReferences.stream(), backgroundReferences.stream()))
        .anyMatch(reference -> reference != null && reference.startsWith("secret://"))) {
      throw new IllegalArgumentException("mounted secret references require secret_directory");
    }
  }

  private static String requireCredentialReference(String value) {
    Objects.requireNonNull(value, "credentialReference");
    if (!(value.matches("env://[A-Z][A-Z0-9_]{0,127}")
        || value.matches("secret://[A-Za-z0-9][A-Za-z0-9._/-]{0,240}"))) {
      throw new IllegalArgumentException("credential reference is invalid");
    }
    return value;
  }

  private static String requireIdentifier(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) {
      throw new IllegalArgumentException(name + " must be a bounded identifier");
    }
    return value;
  }

  private static void requireKerberosPrincipal(String value) {
    if (value == null || value.isBlank() || value.length() > 512
        || value.codePoints().anyMatch(Character::isISOControl)
        || !value.contains("@")) {
      throw new IllegalArgumentException("trusted Kerberos principal is invalid");
    }
  }

  private static <T> List<T> boundedCopy(List<T> values, int maximum, String name) {
    Objects.requireNonNull(values, name);
    if (values.isEmpty() || values.size() > maximum) {
      throw new IllegalArgumentException(name + " count is outside its limit");
    }
    return List.copyOf(values);
  }

  private static void requireUnique(List<String> values, String name) {
    if (new HashSet<>(values).size() != values.size()) {
      throw new IllegalArgumentException(name + " must be unique");
    }
  }

  private static void requireConfiguredProfile(String profileId, List<ClientProfile> profiles) {
    if (profiles.stream().noneMatch(profile -> profile.id().equals(profileId))) {
      throw new IllegalArgumentException("M5 identity references an unknown client profile");
    }
  }
}
