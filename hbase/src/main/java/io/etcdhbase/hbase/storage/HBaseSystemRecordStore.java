package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.SystemRecordStore;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordPage;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.codec.SystemRecordKeyCodec;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.hbase.error.HBaseFailures;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/** Strong-read, exact-CAS adapter for immutable system records; borrowed connections stay shared. */
public final class HBaseSystemRecordStore implements SystemRecordStore {
  public static final int MAX_RECORD_BYTES = 9 * 1024 * 1024;
  private static final int MAX_SCAN_ENTRIES = 1024;
  private static final long MAX_SCAN_BYTES = 8L << 20;
  private static final byte[] VALUE = {'r'};

  private final AsyncConnection connection;
  private final SystemTableBinding binding;
  private final boolean resultsSupported;
  private final boolean auxiliarySupported;
  private final boolean leaseBindingsSupported;

  HBaseSystemRecordStore(AsyncConnection connection, SystemTableBinding binding,
      boolean resultsSupported, boolean auxiliarySupported, boolean leaseBindingsSupported) {
    this.connection = connection;
    this.binding = binding;
    this.resultsSupported = resultsSupported;
    this.auxiliarySupported = auxiliarySupported;
    this.leaseBindingsSupported = leaseBindingsSupported;
  }

  public boolean supportsAuxiliary() { return auxiliarySupported; }
  public boolean supportsLeaseBindings() { return leaseBindingsSupported; }

  @Override
  public int bucket(SystemRecordType type, ByteKey identity) {
    requireTypeSupported(type);
    return binding.keyCodec().bucket(type, identity);
  }

  public static CompletableFuture<HBaseSystemRecordStore> open(
      HBaseConnectionFactory factory,
      AuthorizedBackendIdentity identity,
      SystemTableBinding binding,
      RequestScope scope) {
    var context = new HBaseRequestContext(scope);
    return context.borrowConnection(factory, identity).thenCompose(connection ->
        HBaseRequestCompletion.startRequest(scope,
            () -> connection.getAdmin().getDescriptor(binding.table()))
            .thenApply(descriptor -> {
              boolean supportsResults = binding.supportsResults(descriptor);
              boolean supportsAuxiliary = binding.supportsAuxiliary(descriptor);
              boolean supportsLeaseBindings = binding.supportsLeaseBindings(descriptor);
              return new HBaseSystemRecordStore(connection, binding, supportsResults,
                  supportsAuxiliary, supportsLeaseBindings);
            })).exceptionallyCompose(failure -> CompletableFuture.failedFuture(
                HBaseFailures.classify(failure)));
  }

  @Override
  public CompletableFuture<Optional<ByteKey>> read(
      SystemRecordType type, ByteKey identity, RequestScope scope) {
    requireTypeSupported(type);
    byte[] row = binding.row(type, identity);
    return execute(scope, table -> table.get(
        new Get(row).setConsistency(Consistency.STRONG).addFamily(binding.family())))
        .thenApply(result -> decode(result, binding.family()));
  }

  @Override
  public CompletableFuture<CreateResult> createIfAbsent(
      SystemRecordType type, ByteKey identity, ByteKey value, RequestScope scope) {
    requireTypeSupported(type);
    byte[] row = binding.row(type, identity);
    byte[] bytes = checkedValue(value);
    Put put = put(row, bytes);
    return execute(scope, table -> table.checkAndMutate(
        CheckAndMutate.newBuilder(row).ifNotExists(binding.family(), VALUE).build(put)))
        .thenCompose(result -> result.isSuccess()
            ? CompletableFuture.completedFuture(CreateResult.CREATED)
            : read(type, identity, scope).thenApply(existing -> existing
                .map(found -> Arrays.equals(found.bytes(), bytes)
                    ? CreateResult.ALREADY_PRESENT : CreateResult.CONFLICT)
                .orElseThrow(() -> new CompletionException(
                    new BackendFailure(BackendFailure.Category.UNKNOWN)))));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(
      SystemRecordType type,
      ByteKey identity,
      Optional<ByteKey> expected,
      ByteKey replacement,
      RequestScope scope) {
    requireTypeSupported(type);
    byte[] row = binding.row(type, identity);
    byte[] bytes = checkedValue(replacement);
    var builder = CheckAndMutate.newBuilder(row);
    if (expected.isPresent()) {
      builder.ifEquals(binding.family(), VALUE, checkedValue(expected.orElseThrow()));
    } else {
      builder.ifNotExists(binding.family(), VALUE);
    }
    return execute(scope, table -> table.checkAndMutate(builder.build(put(row, bytes))))
        .thenApply(result -> result.isSuccess());
  }

  @Override
  public CompletableFuture<SystemRecordPage> scanBucket(SystemRecordType type, int bucket,
      Optional<ByteKey> afterIdentity, int maximumEntries, long maximumBytes,
      RequestScope scope) {
    requireTypeSupported(type);
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    if (maximumEntries < 1 || maximumEntries > MAX_SCAN_ENTRIES
        || maximumBytes < 1 || maximumBytes > MAX_SCAN_BYTES) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("system-record scan budget is outside its limit"));
    }
    var bounds = binding.keyCodec().bucketBounds(type, bucket, afterIdentity);
    Scan scan = new Scan()
        .withStartRow(bounds.startRow(), bounds.startInclusive())
        .withStopRow(bounds.stopRow(), bounds.stopInclusive())
        .setLimit(maximumEntries + 1)
        .setCaching(1)
        .setMaxResultSize(maximumBytes)
        .addFamily(binding.family());
    return scan(scan, scope, new PageCollector(type, bucket, maximumEntries, maximumBytes));
  }

  @Override
  public CompletableFuture<SystemRecordPage> scanExpiryWindow(int bucket,
      long startDeadlineInclusive, long stopDeadlineExclusive, Optional<ByteKey> afterIdentity,
      int maximumEntries, long maximumBytes, RequestScope scope) {
    requireTypeSupported(SystemRecordType.EXPIRY_CANDIDATE);
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    if (maximumEntries < 1 || maximumEntries > MAX_SCAN_ENTRIES
        || maximumBytes < 1 || maximumBytes > MAX_SCAN_BYTES) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("expiry scan budget is outside its limit"));
    }
    var bounds = binding.keyCodec().expiryWindowBounds(bucket, startDeadlineInclusive,
        stopDeadlineExclusive, afterIdentity);
    Scan scan = new Scan()
        .withStartRow(bounds.startRow(), bounds.startInclusive())
        .withStopRow(bounds.stopRow(), bounds.stopInclusive())
        .setLimit(maximumEntries + 1)
        .setCaching(1)
        .setMaxResultSize(maximumBytes)
        .addFamily(binding.family());
    return scan(scan, scope, new PageCollector(SystemRecordType.EXPIRY_CANDIDATE, bucket,
        maximumEntries, maximumBytes));
  }

  @Override
  public CompletableFuture<SystemRecordPage> scanLeaseBindings(long leaseId, long incarnation,
      Optional<ByteKey> afterIdentity, int maximumEntries, long maximumBytes, RequestScope scope) {
    requireTypeSupported(SystemRecordType.LEASE_BINDING);
    Objects.requireNonNull(afterIdentity, "afterIdentity");
    if (maximumEntries < 1 || maximumEntries > MAX_SCAN_ENTRIES
        || maximumBytes < 1 || maximumBytes > MAX_SCAN_BYTES) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Lease binding scan budget is outside its limit"));
    }
    var bounds = binding.keyCodec().leaseBindingBounds(leaseId, incarnation, afterIdentity);
    int bucket = binding.keyCodec().leaseBindingBucket(leaseId, incarnation);
    Scan scan = new Scan()
        .withStartRow(bounds.startRow(), bounds.startInclusive())
        .withStopRow(bounds.stopRow(), bounds.stopInclusive())
        .setLimit(maximumEntries + 1)
        .setCaching(1)
        .setMaxResultSize(maximumBytes)
        .addFamily(binding.family());
    return scan(scan, scope,
        new PageCollector(SystemRecordType.LEASE_BINDING, bucket, maximumEntries, maximumBytes));
  }

  @Override
  public CompletableFuture<Optional<SystemRecordEntry>> latestAtOrBefore(
      SystemRecordType type, ByteKey routePrefix, long sequence, RequestScope scope) {
    requireTypeSupported(type);
    Objects.requireNonNull(routePrefix, "routePrefix");
    var bounds = switch (type) {
      case LEASE_STATE -> binding.keyCodec().leaseVersionBounds(type, routePrefix, sequence);
      case LEASE_BINDING -> binding.keyCodec().leaseBindingVersionBounds(routePrefix, sequence);
      default -> throw new IllegalArgumentException("system record type has no versioned seek");
    };
    Scan scan = new Scan()
        .withStartRow(bounds.startRow(), bounds.startInclusive())
        .withStopRow(bounds.stopRow(), bounds.stopInclusive())
        .setReversed(true)
        .setLimit(1)
        .setCaching(1)
        .setMaxResultSize(MAX_RECORD_BYTES)
        .addFamily(binding.family());
    return scan(scan, scope, new LatestVersionCollector(type, routePrefix, sequence));
  }

  private <T> CompletableFuture<T> scan(Scan scan, RequestScope scope,
      ScanCollector<T> collector) {
    return execute(scope, table -> {
      var completion = new CompletableFuture<T>();
      var stoppedByCollector = new AtomicReference<Throwable>();
      table.scan(scan, new AdvancedScanResultConsumer() {
        @Override
        public void onNext(Result[] results, ScanController controller) {
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            stopForClosedScope(scope, controller);
            return;
          }
          collector.onBatch(results.length);
          for (Result result : results) {
            try {
              if (!collector.onResult(result)) {
                controller.terminate();
                return;
              }
            } catch (RuntimeException failure) {
              stoppedByCollector.compareAndSet(null, failure);
              try {
                controller.terminate();
              } catch (RuntimeException terminationFailure) {
                failure.addSuppressed(terminationFailure);
              }
              return;
            }
          }
        }

        @Override
        public void onHeartbeat(ScanController controller) {
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            stopForClosedScope(scope, controller);
          }
        }

        @Override
        public void onError(Throwable error) {
          Throwable collectorFailure = stoppedByCollector.get();
          completion.completeExceptionally(collectorFailure == null
              ? HBaseFailures.classify(error) : collectorFailure);
        }

        @Override
        public void onComplete() {
          if (completion.isDone()) return;
          Throwable collectorFailure = stoppedByCollector.get();
          if (collectorFailure != null) {
            completion.completeExceptionally(collectorFailure);
            return;
          }
          if (scope.isDeadlineExceeded() || scope.isClosed()) {
            completion.completeExceptionally(scanClosureFailure(scope));
            return;
          }
          try {
            completion.complete(collector.complete());
          } catch (RuntimeException failure) {
            completion.completeExceptionally(failure);
          }
        }
      });
      return completion;
    });
  }

  private static void stopForClosedScope(RequestScope scope,
      AdvancedScanResultConsumer.ScanController controller) {
    if (scope.isDeadlineExceeded()) scope.cancel();
    controller.terminate();
  }

  private static BackendFailure scanClosureFailure(RequestScope scope) {
    return new BackendFailure(scope.isDeadlineExceeded()
        ? BackendFailure.Category.TIMEOUT : BackendFailure.Category.CANCELLED);
  }

  private interface ScanCollector<T> {
    /** Returns false when the scan should be stopped after this result. */
    boolean onResult(Result result);

    default void onBatch(int resultCount) { }

    T complete();
  }

  private final class PageCollector implements ScanCollector<SystemRecordPage> {
    private final SystemRecordType type;
    private final int bucket;
    private final int maximumEntries;
    private final long maximumBytes;
    private final ArrayList<SystemRecordEntry> entries = new ArrayList<>();
    private long encodedBytes;
    private long physicalRows;
    private long scanRpcCalls;
    private boolean hasMore;

    private PageCollector(SystemRecordType type, int bucket, int maximumEntries,
        long maximumBytes) {
      this.type = type;
      this.bucket = bucket;
      this.maximumEntries = maximumEntries;
      this.maximumBytes = maximumBytes;
    }

    @Override
    public boolean onResult(Result result) {
      physicalRows++;
      SystemRecordKeyCodec.DecodedKey decoded = binding.keyCodec().decode(result.getRow());
      if (decoded.type() != type || binding.keyCodec().bucket(type, decoded.identity()) != bucket) {
        throw new IllegalArgumentException("system-record scan escaped its type or salt bucket");
      }
      if (entries.size() == maximumEntries) {
        hasMore = true;
        return false;
      }
      int valueLength = validatedValueLength(result);
      long nextBytes = Math.addExact(encodedBytes,
          Math.addExact(decoded.identity().length(), valueLength));
      if (nextBytes > maximumBytes) {
        if (entries.isEmpty()) {
          throw new CompletionException(new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT));
        }
        hasMore = true;
        return false;
      }
      ByteKey value = decode(result, binding.family()).orElseThrow();
      entries.add(new SystemRecordEntry(decoded.identity(), value));
      encodedBytes = nextBytes;
      return true;
    }

    @Override
    public void onBatch(int resultCount) {
      if (resultCount < 1) throw new IllegalArgumentException("empty HBase scan batch");
      scanRpcCalls++;
    }

    @Override
    public SystemRecordPage complete() {
      Optional<ByteKey> lastIdentity = entries.isEmpty()
          ? Optional.empty() : Optional.of(entries.getLast().identity());
      return new SystemRecordPage(entries, lastIdentity, !hasMore, encodedBytes,
          physicalRows, Math.max(1, scanRpcCalls));
    }
  }

  private final class LatestVersionCollector
      implements ScanCollector<Optional<SystemRecordEntry>> {
    private final SystemRecordType type;
    private final ByteKey routePrefix;
    private final long sequence;
    private Optional<SystemRecordEntry> latest = Optional.empty();

    private LatestVersionCollector(SystemRecordType type, ByteKey routePrefix, long sequence) {
      this.type = type;
      this.routePrefix = routePrefix;
      this.sequence = sequence;
    }

    @Override
    public boolean onResult(Result row) {
      if (latest.isPresent()) {
        throw new IllegalStateException("version seek returned more than one system record");
      }
      SystemRecordKeyCodec.DecodedKey decoded = binding.keyCodec().decode(row.getRow());
      if (decoded.type() != type || !startsWith(decoded.identity(), routePrefix)) {
        throw new IllegalArgumentException("version seek escaped its logical route");
      }
      long actualSequence = switch (type) {
        case LEASE_STATE -> io.etcdhbase.core.lease.LeaseRecordIdentity.sequence(decoded.identity());
        case LEASE_BINDING -> io.etcdhbase.core.lease.LeaseBindingIdentity.sequence(decoded.identity());
        default -> throw new IllegalArgumentException("system record type has no versioned sequence");
      };
      if (actualSequence > sequence) {
        throw new IllegalArgumentException("version seek returned a future record version");
      }
      latest = Optional.of(new SystemRecordEntry(decoded.identity(),
          decode(row, binding.family()).orElseThrow()));
      return false;
    }

    @Override
    public Optional<SystemRecordEntry> complete() {
      return latest;
    }
  }

  private static boolean startsWith(ByteKey value, ByteKey prefix) {
    byte[] bytes = value.bytes();
    byte[] prefixBytes = prefix.bytes();
    return bytes.length >= prefixBytes.length
        && Arrays.equals(bytes, 0, prefixBytes.length, prefixBytes, 0, prefixBytes.length);
  }

  static Optional<ByteKey> decode(Result result, byte[] family) {
    if (result.isEmpty()) return Optional.empty();
    int expectedLength = validatedValueLength(result);
    // Check the already-decoded cell before getValue makes another potentially large copy.
    // This does not impose a bound on the HBase client's incoming RPC frame.
    byte[] bytes = result.getValue(family, VALUE);
    if (bytes == null) throw new IllegalArgumentException("system record is missing its value");
    if (bytes.length != expectedLength) {
      throw new IllegalArgumentException("system record value length changed during decode");
    }
    return Optional.of(ByteKey.copyOf(bytes));
  }

  private static int validatedValueLength(Result result) {
    Cell[] cells = result.rawCells();
    if (cells == null || cells.length != 1
        || !CellUtil.matchingQualifier(cells[0], VALUE)) {
      throw new IllegalArgumentException("system record contains an unexpected cell layout");
    }
    int length = cells[0].getValueLength();
    validateValueLength(length);
    return length;
  }

  private Put put(byte[] row, byte[] value) {
    return new Put(row).setDurability(Durability.SYNC_WAL)
        .addColumn(binding.family(), VALUE, value);
  }

  private <T> CompletableFuture<T> execute(
      RequestScope scope,
      Function<AsyncTable<AdvancedScanResultConsumer>, CompletableFuture<T>> operation) {
    return HBaseRequestCompletion.startRequest(scope, () -> {
      long timeout = Math.max(1, Math.min(TimeUnit.SECONDS.toNanos(30), scope.remainingDeadlineNanos()));
      AsyncTable<AdvancedScanResultConsumer> table = connection.getTableBuilder(binding.table())
          .setOperationTimeout(timeout, TimeUnit.NANOSECONDS)
          .setRpcTimeout(timeout, TimeUnit.NANOSECONDS)
          .setReadRpcTimeout(timeout, TimeUnit.NANOSECONDS)
          .setScanTimeout(timeout, TimeUnit.NANOSECONDS)
          .build();
      return operation.apply(table);
    });
  }

  private static byte[] checkedValue(ByteKey value) {
    validateValueLength(value.length());
    return value.bytes();
  }

  private void requireTypeSupported(SystemRecordType type) {
    Objects.requireNonNull(type, "type");
    // RESULT marks the M3 system-record capability, including its private workset pages.
    if ((type == SystemRecordType.RESULT || type == SystemRecordType.TABLE_GROUP_IDENTITY
        || type == SystemRecordType.WORKSET)
        && !resultsSupported) {
      throw new IllegalStateException("system table does not declare M3 record capability");
    }
    if ((type == SystemRecordType.LEASE_STATE || type == SystemRecordType.LEASE_ENUMERATION
        || type == SystemRecordType.EXPIRY_CANDIDATE || type == SystemRecordType.AUXILIARY
        || type == SystemRecordType.LEASE_RESULT || type == SystemRecordType.LEASE_BINDING)
        && !auxiliarySupported) {
      throw new IllegalStateException("system table does not declare M4 auxiliary capability");
    }
    if (type == SystemRecordType.LEASE_BINDING && !leaseBindingsSupported) {
      throw new IllegalStateException("system table does not declare J41 Lease binding capability");
    }
  }

  private static void validateValueLength(int length) {
    if (length == 0 || length > MAX_RECORD_BYTES) {
      throw new IllegalArgumentException("system record value exceeds its storage budget");
    }
  }
}
