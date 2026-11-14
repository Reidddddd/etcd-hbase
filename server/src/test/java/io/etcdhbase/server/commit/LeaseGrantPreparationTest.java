package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.AuxiliaryDirectory;
import io.etcdhbase.core.commit.AuxiliaryPage;
import io.etcdhbase.core.commit.InvocationBinding;
import io.etcdhbase.core.commit.InvocationId;
import io.etcdhbase.core.commit.InvocationRepository;
import io.etcdhbase.core.commit.InvocationService;
import io.etcdhbase.core.commit.ManifestPreparation;
import io.etcdhbase.core.commit.ManifestRepository;
import io.etcdhbase.core.commit.ManifestRoot;
import io.etcdhbase.core.commit.PreparedAuxiliaryRecord;
import io.etcdhbase.core.commit.ResultCatalog;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultIdentity;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.ResultPreparation;
import io.etcdhbase.core.commit.ResultRepository;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.TransactionId;
import io.etcdhbase.core.lease.LeaseGrantResult;
import io.etcdhbase.core.lease.LeaseIdentity;
import io.etcdhbase.core.lease.LeaseState;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.scan.BudgetManager;
import io.etcdhbase.hbase.codec.LeaseGrantResultMessages;
import io.etcdhbase.hbase.codec.LeaseStateMessages;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LeaseGrantPreparationTest {
  @Test
  void sealsOriginalResultAndAllThreeStateRecordsAfterReadValidation() throws Exception {
    try (Fixture fixture = new Fixture()) {
      AtomicBoolean readFinished = new AtomicBoolean();
      var sealed = fixture.preparation.prepare(fixture.invocation, fixture.state,
          fixture.scope, fixture.account, () -> {
            readFinished.set(true);
            return CompletableFuture.completedFuture(null);
          }).join();
      assertTrue(readFinished.get());
      assertEquals(InvocationBinding.State.SEALED, sealed.invocation().state());
      assertFalse(sealed.manifest().containsKeyChanges());
      assertEquals(3, sealed.manifest().auxiliaryReference().orElseThrow().itemCount());
      assertTrue(sealed.manifest().resultReference().isPresent());
      assertEquals(3, fixture.records.size());
      var stateRecord = fixture.records.values().stream()
          .filter(record -> record.ordinal() == 0).findFirst().orElseThrow();
      assertEquals(fixture.state, LeaseStateMessages.decode(stateRecord.payload()));
      assertEquals(new LeaseGrantResult(-7, 30, 1),
          LeaseGrantResultMessages.decode(ByteKey.copyOf(fixture.results.page.payload())));
    }
  }

  @Test
  void failedReadProtectionDoesNotSealOrAdvanceInvocation() throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertThrows(CompletionException.class, () -> fixture.preparation.prepare(
          fixture.invocation, fixture.state, fixture.scope, fixture.account,
          () -> CompletableFuture.failedFuture(new IllegalStateException("lost pin"))).join());
      assertEquals(0, fixture.seals);
      assertEquals(InvocationBinding.State.PREPARING, fixture.current.get().state());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final RequestScope scope = RequestScope.withTimeout(Duration.ofSeconds(10));
    private final InvocationBinding invocation = InvocationBinding.begin(InvocationId.create(),
        ByteKey.copyOf(new byte[32]), ByteKey.copyOf(new byte[32]), TransactionId.create(), 1, 0, 1);
    private final LeaseState state = LeaseState.grant(new LeaseIdentity(-7, 1), 30, 40_000, 1);
    private final AtomicReference<InvocationBinding> current = new AtomicReference<>(invocation);
    private final Map<ByteKey, PreparedAuxiliaryRecord> records = new ConcurrentHashMap<>();
    private final Map<ByteKey, AuxiliaryPage> pages = new ConcurrentHashMap<>();
    private final Map<ByteKey, AuxiliaryDirectory> directories = new ConcurrentHashMap<>();
    private final Results results = new Results();
    private final BudgetManager budgets;
    private final BudgetManager.Account account;
    private final LeaseGrantPreparation preparation;
    private volatile int seals;

    private Fixture() throws Exception {
      var limits = new BudgetManager.Limits(2, 0, 2, 1, 1, 0, 16L << 20);
      budgets = new BudgetManager(limits, Map.of("group", limits), limits);
      account = budgets.admit("group", scope, new BudgetManager.Reservation(0, 1,
          1L << 20, 1, 2L << 20));
      ManifestRepository manifestRepository = (ManifestRepository) Proxy.newProxyInstance(
          ManifestRepository.class.getClassLoader(), new Class<?>[] {ManifestRepository.class},
          (proxy, method, args) -> switch (method.getName()) {
            case "createAuxiliaryRecord" -> {
              var record = (PreparedAuxiliaryRecord) args[0];
              records.put(record.systemIdentity(), record);
              yield created();
            }
            case "createAuxiliaryPage" -> {
              var page = (AuxiliaryPage) args[0];
              pages.put(page.systemIdentity(), page);
              yield created();
            }
            case "createAuxiliaryDirectory" -> {
              var directory = (AuxiliaryDirectory) args[0];
              directories.put(directory.systemIdentity(), directory);
              yield created();
            }
            case "seal" -> { seals++; yield created(); }
            default -> throw new AssertionError("unexpected manifest operation " + method.getName());
          });
      InvocationRepository invocationRepository = new InvocationRepository() {
        @Override public CompletableFuture<Optional<InvocationBinding>> read(InvocationId id, RequestScope ignored) {
          return CompletableFuture.completedFuture(Optional.of(current.get()));
        }
        @Override public CompletableFuture<SystemRecordStore.CreateResult> createIfAbsent(
            InvocationBinding binding, RequestScope ignored) { throw new AssertionError("unexpected create"); }
        @Override public CompletableFuture<Boolean> compareAndSet(InvocationBinding expected,
            InvocationBinding replacement, RequestScope ignored) {
          return CompletableFuture.completedFuture(current.compareAndSet(expected, replacement));
        }
      };
      preparation = new LeaseGrantPreparation(new ResultPreparation(results), new ResultCatalog(results),
          new ManifestPreparation(manifestRepository, new ManifestPreparation.Limits(0, 0, 0, 0)),
          new InvocationService(invocationRepository));
    }

    private static CompletableFuture<SystemRecordStore.CreateResult> created() {
      return CompletableFuture.completedFuture(SystemRecordStore.CreateResult.CREATED);
    }

    @Override public void close() {
      scope.close();
      scope.cleanupCompletion().toCompletableFuture().join();
      scope.physicalProducersCompletion().toCompletableFuture().join();
      assertEquals(0, budgets.snapshot().allocatedBytes());
    }
  }

  private static final class Results implements ResultRepository {
    private volatile ResultPage page;
    private volatile ResultDirectory directory;
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createPage(ResultPage value, RequestScope scope) {
      page = value; return Fixture.created();
    }
    @Override public CompletableFuture<SystemRecordStore.CreateResult> createDirectory(ResultDirectory value, RequestScope scope) {
      directory = value; return Fixture.created();
    }
    @Override public CompletableFuture<Optional<ResultPage>> readPage(ResultIdentity id, int index, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(page));
    }
    @Override public CompletableFuture<Optional<ResultDirectory>> readDirectory(ResultIdentity id, RequestScope scope) {
      return CompletableFuture.completedFuture(Optional.ofNullable(directory));
    }
  }
}
