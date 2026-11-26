package io.etcdhbase.server;

import io.etcdhbase.core.commit.CommitOutcomeResolver;
import io.etcdhbase.core.commit.CommitOwnerService;
import io.etcdhbase.core.commit.CommitPlanVerifier;
import io.etcdhbase.core.commit.CommitPublisher;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.ManifestCatalog;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.identity.HBaseAuthenticators;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.hbase.storage.HBaseCommitControlStore;
import io.etcdhbase.hbase.storage.HBaseCommitDecisionRepository;
import io.etcdhbase.hbase.storage.HBaseInvocationRepository;
import io.etcdhbase.hbase.storage.HBaseManifestRepository;
import io.etcdhbase.hbase.storage.HBaseMaterializationStore;
import io.etcdhbase.hbase.storage.HBaseResultRepository;
import io.etcdhbase.hbase.storage.HBaseSystemRecordStore;
import io.etcdhbase.hbase.storage.StorageTableBinding;
import io.etcdhbase.hbase.storage.SystemTableBinding;
import io.etcdhbase.server.commit.BoundedPublicationHandoff;
import io.etcdhbase.server.commit.CommitAuthorizationProofTestSupport;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.BackendCredentials;
import io.etcdhbase.spi.salt.HashSaltStrategy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;

/** Test subprocess that exits immediately after a real publication CAS is observed. */
public final class M3CrashWriterMain {
  private static final int SELECTED_EXIT = 77;

  private M3CrashWriterMain() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 7) throw new IllegalArgumentException("invalid fixture arguments");
    var configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(Path.of(arguments[0]).toUri()));
    var authentication = new AuthenticationScope(arguments[1], "public");
    var salt = new HashSaltStrategy();
    var system = new SystemTableBinding(TableName.valueOf(arguments[2]), new byte[] {'s'},
        authentication.tableGroupId(), arguments[3], salt);
    var layout = StorageTableBinding.Layout.valueOf(arguments[4]);
    var primary = new StorageTableBinding(TableName.valueOf(arguments[5]), new byte[] {'d'}, layout, salt);
    var current = arguments[6].equals("-") ? Optional.<StorageTableBinding>empty()
        : Optional.of(new StorageTableBinding(TableName.valueOf(arguments[6]), new byte[] {'d'},
            StorageTableBinding.Layout.CURRENT, salt));
    InvocationId invocationId = new InvocationId(UUID.fromString(System.getenv("M3_FIXTURE_INVOCATION")));
    try (var provider = new HBaseBackendIdentityProvider("m3-crash-child", authentication,
            configuration, HBaseAuthenticators.simpleDevelopment(),
            (connection, ignored) -> connection.getAdmin().listNamespaces().thenApply(names -> null),
            1, 1, Duration.ofSeconds(30));
        var credentials = new BackendCredentials("m3-crash-child", new char[0]);
        var scope = RequestScope.withTimeout(Duration.ofMinutes(2));
        var handoff = new BoundedPublicationHandoff(1, 1, Duration.ofSeconds(30))) {
      var identity = provider.authenticate(credentials, authentication).toCompletableFuture().join();
      var records = HBaseSystemRecordStore.open(provider, identity, system, scope).join();
      var control = HBaseCommitControlStore.open(provider, identity, system, scope).join();
      var invocations = new HBaseInvocationRepository(records);
      var binding = invocations.read(invocationId, scope).join().orElseThrow();
      var manifests = new ManifestCatalog(new HBaseManifestRepository(records));
      var manifest = manifests.readRoot(binding.manifestRoot().orElseThrow(), scope).join().orElseThrow();
      var results = new ResultCatalog(new HBaseResultRepository(records));
      var materialization = HBaseMaterializationStore.openForTableGroup(
          provider, identity, system, primary, current, scope).join();
      var decisions = new HBaseCommitDecisionRepository(records);
      var outcomes = new CommitOutcomeResolver(control, decisions);
      long now = System.currentTimeMillis();
      var owner = new CommitOwnerService(control).claim("m3-crash-child", now, now + 6_000, scope)
          .join().orElseThrow();
      if (owner.epoch() != binding.preparedOwnerEpoch()) {
        throw new IllegalStateException("unexpected owner epoch in crash fixture");
      }
      var publisher = new CommitPublisher(control, outcomes,
          new CommitPlanVerifier(manifests, materialization, results), invocations, handoff,
          (selectedBinding, publication, serviceScope) -> {
            if (publication.state() != CommitOutcomeResolver.State.SELECTED_PENDING) {
              return CompletableFuture.failedFuture(new IllegalStateException("expected selected publication"));
            }
            System.out.println("m3-crash-fixture: selected and stopping before materialization");
            System.out.flush();
            Runtime.getRuntime().halt(SELECTED_EXIT);
            return CompletableFuture.failedFuture(new AssertionError("process halt returned"));
          });
      var controlState = control.read(scope).join().orElseThrow().state();
      var authorization = CommitAuthorizationProofTestSupport.serviceProof(
          identity, binding.authorizationScopeDigest(), controlState.authorizationBarrierGeneration());
      publisher.publish(binding, owner, manifest, scope, authorization).join();
      throw new IllegalStateException("selected crash fixture did not halt");
    }
  }
}
