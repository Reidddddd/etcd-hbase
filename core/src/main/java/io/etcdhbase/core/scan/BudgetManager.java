package io.etcdhbase.core.scan;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.StoragePage;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Single ledger for global/group/request admission, work and reserved memory lanes. */
public final class BudgetManager {
  public enum Memory { HEAD, PAGE, IO, OUTPUT, CANDIDATE }

  public record Limits(int requests, int queued, int operations, int scanners, int cleanup,
      long queuedBytes, long memoryBytes) {
    public Limits {
      if (requests < 1 || queued < 0 || operations < 1 || scanners < 1 || cleanup < 1
          || queuedBytes < 0 || memoryBytes < 1) throw new IllegalArgumentException("invalid resource limits");
    }
  }

  public record Reservation(long heads, long page, long io, long output, long candidates) {
    /** Existing reservations fail closed for candidate buffers until callers reserve them. */
    public Reservation(long heads, long page, long io, long output) {
      this(heads, page, io, output, 0);
    }

    public Reservation {
      if (heads < 0 || page < 1 || io < 1 || output < 0 || candidates < 0) {
        throw new IllegalArgumentException("invalid memory reservation");
      }
      try {
        Math.addExact(Math.addExact(Math.addExact(heads, page), Math.addExact(io, output)), candidates);
      } catch (ArithmeticException overflow) {
        throw new IllegalArgumentException("memory reservation overflow", overflow);
      }
    }

    public long total() { return heads + page + io + output + candidates; }

    long capacity(Memory kind) {
      return switch (kind) {
        case HEAD -> heads;
        case PAGE -> page;
        case IO -> io;
        case OUTPUT -> output;
        case CANDIDATE -> candidates;
      };
    }
  }

  public record Work(int queued, int operations, int scanners, int cleanup, long queuedBytes) {
    public Work {
      if (queued < 0 || operations < 0 || scanners < 0 || cleanup < 0 || queuedBytes < 0) {
        throw new IllegalArgumentException("negative resource claim");
      }
    }
    public static Work scan() { return new Work(0, 1, 1, 1, 0); }
    public static Work queued(long bytes) { return new Work(1, 0, 0, 0, bytes); }
  }

  public record Snapshot(long requests, long queued, long operations, long scanners,
      long cleanup, long queuedBytes, long reservedBytes, long allocatedBytes, long retiredScanners,
      long peakScanners, long peakAllocatedBytes) {}

  /** Per-request read work; cellBytes includes returned cells and fetched manifest candidates. */
  public record ReadLimits(long physicalRows, long cellBytes, long scanRpcCalls, long pages, long pointOperations) {
    public ReadLimits {
      if (physicalRows < 1 || cellBytes < 1 || scanRpcCalls < 1 || pages < 1 || pointOperations < 1) throw new IllegalArgumentException("invalid read limits");
    }
  }

  private static final long DEFAULT_PAGE_LIMIT = 65_536;
  private static final long DEFAULT_POINT_OPERATION_LIMIT = 65_536;
  private static final Work NO_WORK = new Work(0, 0, 0, 0, 0);
  private final Limits globalLimits;
  private final Limits requestLimits;
  private final Map<String, Group> groups;
  private final Counters global = new Counters();
  private Runnable wakeup;

  public BudgetManager(Limits globalLimits, Map<String, Limits> groupLimits, Limits requestLimits) {
    this.globalLimits = Objects.requireNonNull(globalLimits, "globalLimits");
    this.requestLimits = Objects.requireNonNull(requestLimits, "requestLimits");
    if (groupLimits.isEmpty() || groupLimits.size() > 64) throw new IllegalArgumentException("bounded groups required");
    var configured = new java.util.HashMap<String, Group>();
    groupLimits.forEach((id, limits) -> {
      if (id == null || !id.matches("[A-Za-z][A-Za-z0-9._-]{0,63}")) throw new IllegalArgumentException("invalid group ID");
      configured.put(id, new Group(Objects.requireNonNull(limits, "group limits")));
    });
    groups = Map.copyOf(configured);
  }

  public Account admit(String groupId, RequestScope scope, Reservation reservation) throws BackendFailure {
    return admit(groupId, scope, reservation,
        new ReadLimits(1_000_000, 1L << 30, 100_000, DEFAULT_PAGE_LIMIT, DEFAULT_POINT_OPERATION_LIMIT));
  }

  public Account admit(String groupId, RequestScope scope, Reservation reservation, ReadLimits readLimits) throws BackendFailure {
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(reservation, "reservation");
    Objects.requireNonNull(readLimits, "readLimits");
    Account account;
    synchronized (this) {
      Group group = groups.get(groupId);
      if (group == null) throw new IllegalArgumentException("unconfigured group ID");
      checkScope(scope);
      if (reservation.total() > requestLimits.memoryBytes()
          || !global.canAdmit(globalLimits, reservation.total()) || !group.usage.canAdmit(group.limits, reservation.total())) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      global.admit(reservation.total());
      group.usage.admit(reservation.total());
      account = new Account(groupId, group, scope, reservation, readLimits);
    }
    account.registration = scope.registerScopedCleanup(account);
    return account;
  }

  /** Reserves long-lived service memory without consuming a request concurrency slot. */
  public ServiceMemory reserveServiceMemory(String groupId, RequestScope scope, long bytes) throws BackendFailure {
    Objects.requireNonNull(scope, "scope");
    if (bytes <= 0) throw new IllegalArgumentException("service reservation must be positive");
    ServiceMemory memory;
    synchronized (this) {
      Group group = groups.get(groupId);
      if (group == null) throw new IllegalArgumentException("unconfigured group ID");
      checkScope(scope);
      if (bytes > globalLimits.memoryBytes() - global.reservedBytes
          || bytes > group.limits.memoryBytes() - group.usage.reservedBytes) {
        throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
      global.reservedBytes += bytes;
      group.usage.reservedBytes += bytes;
      global.allocatedBytes += bytes;
      group.usage.allocatedBytes += bytes;
      global.peakAllocatedBytes = Math.max(global.peakAllocatedBytes, global.allocatedBytes);
      group.usage.peakAllocatedBytes = Math.max(group.usage.peakAllocatedBytes, group.usage.allocatedBytes);
      memory = new ServiceMemory(group, scope, bytes);
    }
    try {
      memory.registration = scope.registerScopedCleanup(memory);
      checkScope(scope);
      return memory;
    } catch (BackendFailure | RuntimeException | Error failure) {
      try { memory.close(); }
      catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
      throw failure;
    }
  }

  /** A service reservation is bound to one manager, table group and lifecycle. */
  public final class ServiceMemory implements AutoCloseable {
    private final Group group;
    private final RequestScope scope;
    private final long bytes;
    private RequestScope.ScopedCleanup registration;
    private boolean closed;

    private ServiceMemory(Group group, RequestScope scope, long bytes) {
      this.group = group;
      this.scope = scope;
      this.bytes = bytes;
    }

    public boolean permits(Account account) {
      Objects.requireNonNull(account, "account");
      synchronized (BudgetManager.this) {
        return isActive() && account.owner() == BudgetManager.this && account.group == group;
      }
    }

    public boolean isActive() {
      synchronized (BudgetManager.this) {
        return !closed && !scope.isClosed() && !scope.isDeadlineExceeded();
      }
    }

    @Override public void close() {
      synchronized (BudgetManager.this) {
        if (closed) return;
        closed = true;
        global.reservedBytes -= bytes;
        group.usage.reservedBytes -= bytes;
        global.allocatedBytes -= bytes;
        group.usage.allocatedBytes -= bytes;
      }
      try {
        if (registration != null) registration.close();
      } catch (Exception cleanupFailure) {
        throw new IllegalStateException("service reservation cleanup failed", cleanupFailure);
      } finally {
        changed();
      }
    }
  }

  public synchronized Snapshot snapshot() { return global.snapshot(); }

  boolean owns(Account account) { return account.owner() == this; }
  int cleanupCapacity() { return globalLimits.cleanup(); }

  synchronized AutoCloseable onRelease(Runnable listener) {
    if (wakeup != null) throw new IllegalStateException("budget already has a scheduler");
    wakeup = Objects.requireNonNull(listener, "listener");
    return () -> {
      synchronized (BudgetManager.this) {
        if (wakeup == listener) wakeup = null;
      }
    };
  }

  private void changed() {
    Runnable listener;
    synchronized (this) { listener = wakeup; }
    if (listener != null) listener.run();
  }

  private static void checkScope(RequestScope scope) throws BackendFailure {
    if (scope.isClosed()) throw new BackendFailure(BackendFailure.Category.CANCELLED);
    if (scope.isDeadlineExceeded()) throw new BackendFailure(BackendFailure.Category.TIMEOUT);
  }

  public final class Account implements AutoCloseable {
    private final String groupId;
    private final Group group;
    private final RequestScope scope;
    private final Reservation reservation;
    private final Counters usage = new Counters();
    private final EnumMap<Memory, Long> memory = new EnumMap<>(Memory.class);
    private RequestScope.ScopedCleanup registration;
    private boolean closed;
    private boolean admissionReleased;
    private final ReadLimits readLimits;
    private long physicalRows, cellBytes, candidateBytes, reportedCandidateBytes, scanRpcCalls, pages;
    private long pointReads;
    private boolean readExhausted;
    private final CompletableFuture<Void> drained = new CompletableFuture<>();

    private Account(String groupId, Group group, RequestScope scope, Reservation reservation, ReadLimits readLimits) {
      this.groupId = groupId;
      this.group = group;
      this.scope = scope;
      this.reservation = reservation;
      this.readLimits = readLimits;
      for (Memory kind : Memory.values()) memory.put(kind, 0L);
    }

    String groupId() { return groupId; }
    private BudgetManager owner() { return BudgetManager.this; }
    RequestScope scope() { return scope; }
    public boolean belongsTo(RequestScope expectedScope) { return scope == expectedScope; }
    public Reservation reservation() { return reservation; }
    public CompletionStage<Void> termination() { return drained.minimalCompletionStage(); }
    public long pointReads() { synchronized (BudgetManager.this) { return pointReads; } }
    void chargePoint() throws BackendFailure {
      synchronized (BudgetManager.this) {
        requireActive();
        if (pointReads >= readLimits.pointOperations()) {
          readExhausted = true;
          throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        }
        pointReads++;
      }
    }

    /** Charges a bounded internal system-record Get before its value is fetched. */
    public void chargeInternalPointRead(long maximumBytes) throws BackendFailure {
      if (maximumBytes < 0) throw new IllegalArgumentException("negative internal read bytes");
      synchronized (BudgetManager.this) {
        requireActive();
        if (pointReads >= readLimits.pointOperations() || maximumBytes > readLimits.cellBytes() - cellBytes) {
          readExhausted = true;
          throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        }
        pointReads++;
        cellBytes = Math.addExact(cellBytes, maximumBytes);
      }
    }

    /** Charges one bounded internal scan page using its returned physical cost. */
    public void chargeInternalScanPage(long physicalRows, long returnedCellBytes,
        long scanRpcCalls) throws BackendFailure {
      if (physicalRows < 0 || returnedCellBytes < 0 || scanRpcCalls < 1) {
        throw new IllegalArgumentException("invalid internal scan-page cost");
      }
      synchronized (BudgetManager.this) {
        requireActive();
        boolean exceeds = physicalRows > readLimits.physicalRows() - this.physicalRows
            || returnedCellBytes > readLimits.cellBytes() - cellBytes
            || scanRpcCalls > readLimits.scanRpcCalls() - this.scanRpcCalls
            || pages >= readLimits.pages();
        if (exceeds) {
          readExhausted = true;
          throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        }
        this.physicalRows = Math.addExact(this.physicalRows, physicalRows);
        cellBytes = Math.addExact(cellBytes, returnedCellBytes);
        this.scanRpcCalls = Math.addExact(this.scanRpcCalls, scanRpcCalls);
        pages++;
      }
    }

    /** Charges a candidate before its value is fetched so limits bound attempted bytes too. */
    public void chargeCandidateRead(long bytes) throws BackendFailure {
      if (bytes < 0) throw new IllegalArgumentException("negative candidate read bytes");
      synchronized (BudgetManager.this) {
        requireActive();
        if (bytes > readLimits.cellBytes() - cellBytes) {
          readExhausted = true;
          throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        }
        cellBytes = Math.addExact(cellBytes, bytes);
        candidateBytes = Math.addExact(candidateBytes, bytes);
      }
    }
    public boolean isActive() {
      synchronized (BudgetManager.this) { return !closed && !readExhausted && !scope.isClosed() && !scope.isDeadlineExceeded(); }
    }

    void charge(StoragePage.Cost cost) throws BackendFailure {
      synchronized (BudgetManager.this) {
        requireActive();
        long pageCandidateBytes = cost.observations().getOrDefault(
            StoragePage.Observation.MANIFEST_CANDIDATE_BYTES, 0L);
        boolean exceeds = cost.physicalRows() > readLimits.physicalRows() - physicalRows
            || cost.returnedCellBytes() > readLimits.cellBytes() - cellBytes
            || cost.scanRpcCalls() > readLimits.scanRpcCalls() - scanRpcCalls || pages >= readLimits.pages();
        if (exceeds) {
          readExhausted = true;
          throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
        }
        if (pageCandidateBytes > candidateBytes - reportedCandidateBytes) {
          throw new IllegalStateException("manifest page reports candidate bytes that were not fetched");
        }
        physicalRows += cost.physicalRows();
        cellBytes = Math.addExact(cellBytes, cost.returnedCellBytes());
        reportedCandidateBytes = Math.addExact(reportedCandidateBytes, pageCandidateBytes);
        scanRpcCalls += cost.scanRpcCalls();
        pages++;
      }
    }

    public Optional<Lease> tryAcquire(Work work) throws BackendFailure {
      Objects.requireNonNull(work, "work");
      synchronized (BudgetManager.this) {
        requireActive();
        if (!global.fits(globalLimits, work) || !group.usage.fits(group.limits, work) || !usage.fits(requestLimits, work)) {
          return Optional.empty();
        }
        global.add(work, 1);
        group.usage.add(work, 1);
        usage.add(work, 1);
        return Optional.of(new Lease(this, work, null, 0));
      }
    }

    public Optional<Lease> tryRetain(Memory kind, long bytes) throws BackendFailure {
      Objects.requireNonNull(kind, "kind");
      if (bytes < 0) throw new IllegalArgumentException("negative retained bytes");
      synchronized (BudgetManager.this) {
        requireActive();
        if (bytes > reservation.capacity(kind) - memory.get(kind)) return Optional.empty();
        memory.put(kind, memory.get(kind) + bytes);
        global.allocatedBytes += bytes;
        group.usage.allocatedBytes += bytes;
        usage.allocatedBytes += bytes;
        global.peakAllocatedBytes = Math.max(global.peakAllocatedBytes, global.allocatedBytes);
        group.usage.peakAllocatedBytes = Math.max(group.usage.peakAllocatedBytes, group.usage.allocatedBytes);
        usage.peakAllocatedBytes = Math.max(usage.peakAllocatedBytes, usage.allocatedBytes);
        return Optional.of(new Lease(this, NO_WORK, kind, bytes));
      }
    }

    void requireActive() throws BackendFailure {
      synchronized (BudgetManager.this) {
        checkScope(scope);
        if (closed) throw new BackendFailure(BackendFailure.Category.CANCELLED);
        if (readExhausted) throw new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT);
      }
    }

    private void releaseAdmissionIfDrained() {
      if (!closed || admissionReleased || !usage.empty()) return;
      admissionReleased = true;
      global.requests--;
      group.usage.requests--;
      global.reservedBytes -= reservation.total();
      group.usage.reservedBytes -= reservation.total();
    }

    private void signalDrain() {
      boolean finished;
      synchronized (BudgetManager.this) { finished = admissionReleased; }
      if (finished) drained.complete(null);
    }

    @Override
    public void close() {
      synchronized (BudgetManager.this) {
        if (closed) return;
        closed = true;
        releaseAdmissionIfDrained();
      }
      try {
        if (registration != null) registration.close();
      } catch (Exception cleanupFailure) {
        throw new IllegalStateException("budget registration cleanup failed", cleanupFailure);
      } finally {
        signalDrain();
        changed();
      }
    }
  }

  public final class Lease implements AutoCloseable {
    private final Account account;
    private final Work work;
    private final Memory kind;
    private final long bytes;
    private boolean released;

    private Lease(Account account, Work work, Memory kind, long bytes) {
      this.account = account;
      this.work = work;
      this.kind = kind;
      this.bytes = bytes;
    }

    @Override
    public void close() { release(true); }

    void rollback() { release(false); }

    private void release(boolean notify) {
      synchronized (BudgetManager.this) {
        if (released) return;
        released = true;
        global.add(work, -1);
        account.group.usage.add(work, -1);
        account.usage.add(work, -1);
        if (kind != null) {
          account.memory.put(kind, account.memory.get(kind) - bytes);
          global.allocatedBytes -= bytes;
          account.group.usage.allocatedBytes -= bytes;
          account.usage.allocatedBytes -= bytes;
        }
        account.releaseAdmissionIfDrained();
      }
      account.signalDrain();
      if (notify) changed();
    }

    /** Unknown cleanup consumes bounded capacity until the service is replaced/recovered. */
    void retire() {
      synchronized (BudgetManager.this) {
        if (released) return;
        released = true;
        global.retiredScanners += work.scanners();
        account.group.usage.retiredScanners += work.scanners();
      }
      changed();
    }
  }

  private record Group(Limits limits, Counters usage) {
    Group(Limits limits) { this(limits, new Counters()); }
  }

  private static final class Counters {
    long requests, queued, operations, scanners, cleanup, queuedBytes, reservedBytes, allocatedBytes, retiredScanners;
    long peakScanners, peakAllocatedBytes;

    boolean canAdmit(Limits limits, long bytes) {
      return requests < limits.requests() && bytes <= limits.memoryBytes() - reservedBytes;
    }
    void admit(long bytes) { requests++; reservedBytes += bytes; }
    boolean fits(Limits limits, Work work) {
      return work.queued() <= limits.queued() - queued && work.operations() <= limits.operations() - operations
          && work.scanners() <= limits.scanners() - scanners && work.cleanup() <= limits.cleanup() - cleanup
          && work.queuedBytes() <= limits.queuedBytes() - queuedBytes;
    }
    void add(Work work, int sign) {
      queued += sign * (long) work.queued();
      operations += sign * (long) work.operations();
      scanners += sign * (long) work.scanners();
      cleanup += sign * (long) work.cleanup();
      queuedBytes += sign * work.queuedBytes();
      peakScanners = Math.max(peakScanners, scanners);
    }
    boolean empty() { return queued == 0 && operations == 0 && scanners == 0 && cleanup == 0 && allocatedBytes == 0; }
    Snapshot snapshot() { return new Snapshot(requests, queued, operations, scanners, cleanup, queuedBytes, reservedBytes,
        allocatedBytes, retiredScanners, peakScanners, peakAllocatedBytes); }
  }
}
