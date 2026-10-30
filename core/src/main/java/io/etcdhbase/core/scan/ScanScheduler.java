package io.etcdhbase.core.scan;

import io.etcdhbase.core.error.BackendFailure;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.core.storage.ScanCleanupFailure;
import io.etcdhbase.core.storage.ScanOperation;
import io.etcdhbase.core.storage.ScanRequest;
import io.etcdhbase.core.storage.ScanSource;
import io.etcdhbase.core.storage.StoragePage;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Fair finite-page dispatch. Resource counts live exclusively in BudgetManager. */
public final class ScanScheduler implements AutoCloseable {
  private final BudgetManager budget;
  private final long queueWaitNanos;
  private final Executor cleanup;
  private final ScheduledExecutorService timer;
  private final boolean ownsExecutors;
  private final AutoCloseable wakeupRegistration;
  private final Map<String, GroupQueue> groups = new HashMap<>();
  private final ArrayDeque<String> groupOrder = new ArrayDeque<>();
  private final Set<Job> jobs = Collections.newSetFromMap(new IdentityHashMap<>());
  private final AtomicInteger draining = new AtomicInteger();
  private final CompletableFuture<Void> termination = new CompletableFuture<>();
  private boolean closed;
  private Throwable cleanupFailure;

  public ScanScheduler(BudgetManager budget, Duration maximumQueueWait) {
    this(budget, checkedQueueWait(maximumQueueWait), cleanupExecutor(budget), timerExecutor(), true);
  }

  ScanScheduler(BudgetManager budget, Duration maximumQueueWait, Executor cleanup, ScheduledExecutorService timer) {
    this(budget, maximumQueueWait, cleanup, timer, false);
  }

  private ScanScheduler(BudgetManager budget, Duration maximumQueueWait, Executor cleanup,
      ScheduledExecutorService timer, boolean ownsExecutors) {
    this.budget = Objects.requireNonNull(budget, "budget");
    if (maximumQueueWait.isZero() || maximumQueueWait.isNegative() || maximumQueueWait.compareTo(Duration.ofMinutes(5)) > 0) {
      throw new IllegalArgumentException("queue wait must be positive and at most five minutes");
    }
    queueWaitNanos = maximumQueueWait.toNanos();
    this.cleanup = Objects.requireNonNull(cleanup, "cleanup");
    this.timer = Objects.requireNonNull(timer, "timer");
    this.ownsExecutors = ownsExecutors;
    try {
      wakeupRegistration = budget.onRelease(this::pump);
    } catch (RuntimeException failure) {
      if (ownsExecutors) { timer.shutdownNow(); ((ThreadPoolExecutor) cleanup).shutdownNow(); }
      throw failure;
    }
  }

  public ScanSource bind(ScanSource source, BudgetManager.Account account) {
    Objects.requireNonNull(source, "source");
    if (!budget.owns(account)) throw new IllegalArgumentException("account belongs to another budget");
    return request -> submit(source, account, request);
  }

  private ScanOperation submit(ScanSource source, BudgetManager.Account account, ScanRequest request) {
    Job job = new Job(source, account, request);
    try {
      long queueBytes = 256L + request.after().map(key -> key.length()).orElse(0)
          + request.range().lowerInclusive().map(key -> key.length()).orElse(0)
          + request.range().upperExclusive().map(key -> key.length()).orElse(0);
      job.queueLease = account.tryAcquire(BudgetManager.Work.queued(queueBytes)).orElseThrow(ScanScheduler::resourceLimit);
      synchronized (this) {
        if (closed) throw new RejectedExecutionException("scheduler is closed");
        jobs.add(job);
        GroupQueue group = groups.computeIfAbsent(account.groupId(), id -> {
          groupOrder.add(id);
          return new GroupQueue();
        });
        group.add(job);
        job.expiry = timer.schedule(job::expire, Math.min(queueWaitNanos, account.scope().remainingDeadlineNanos()), TimeUnit.NANOSECONDS);
      }
      job.registration = account.scope().registerScopedCleanup(job);
      boolean alreadyDisposed;
      synchronized (this) { alreadyDisposed = job.disposed; }
      if (alreadyDisposed) job.closeRegistration();
      pump();
    } catch (BackendFailure | RuntimeException problem) {
      job.reject(problem);
    } catch (Error fatal) {
      job.reject(fatal);
      throw fatal;
    }
    return job;
  }

  private void pump() {
    if (draining.getAndIncrement() != 0) return;
    int missed = 1;
    do {
      Job job;
      while ((job = takeReady()) != null) job.start();
      missed = draining.addAndGet(-missed);
    } while (missed != 0);
  }

  private synchronized Job takeReady() {
    if (closed) return null;
    int remainingGroups = groupOrder.size();
    while (remainingGroups-- > 0) {
      String id = groupOrder.remove();
      groupOrder.add(id);
      Job ready = readyInGroup(groups.get(id));
      if (ready == null) continue;
      removeQueued(ready);
      ready.running = true;
      if (ready.expiry != null) ready.expiry.cancel(false);
      return ready;
    }
    return null;
  }

  private Job readyInGroup(GroupQueue group) {
    int remainingRequests = group.order.size();
    while (remainingRequests-- > 0) {
      var account = group.order.remove();
      group.order.add(account);
      Job job = group.queues.get(account).getFirst();
      if (!account.isActive()) return job;
      try {
        job.resources = acquire(account, job.request.maximumBytes());
      } catch (BackendFailure failure) {
        job.startFailure = failure;
        return job;
      }
      if (job.resources != null) return job;
    }
    return null;
  }

  private Resources acquire(BudgetManager.Account account, long pageBytes) throws BackendFailure {
    List<BudgetManager.Lease> acquired = new ArrayList<>(3);
    boolean transfer = false;
    try {
      var page = account.tryRetain(BudgetManager.Memory.PAGE, pageBytes);
      if (page.isEmpty()) return null;
      acquired.add(page.get());
      var io = account.tryRetain(BudgetManager.Memory.IO, account.reservation().io());
      if (io.isEmpty()) return null;
      acquired.add(io.get());
      var work = account.tryAcquire(BudgetManager.Work.scan());
      if (work.isEmpty()) return null;
      acquired.add(work.get());
      transfer = true;
      return new Resources(page.get(), io.get(), work.get());
    } finally {
      if (!transfer) for (var lease : acquired) lease.rollback();
    }
  }

  private void removeQueued(Job job) {
    GroupQueue group = groups.get(job.account.groupId());
    if (group == null) return;
    var queue = group.queues.get(job.account);
    if (queue == null) return;
    queue.remove(job);
    if (queue.isEmpty()) {
      group.queues.remove(job.account);
      group.order.remove(job.account);
    }
    if (group.queues.isEmpty()) {
      groups.remove(job.account.groupId());
      groupOrder.remove(job.account.groupId());
    }
  }

  public CompletionStage<Void> termination() { return termination.minimalCompletionStage(); }

  @Override
  public void close() {
    List<Job> owned;
    synchronized (this) {
      if (closed) return;
      closed = true;
      owned = List.copyOf(jobs);
    }
    try {
      for (Job job : owned) job.close();
    } finally {
      try {
        wakeupRegistration.close();
      } catch (Exception failure) {
        throw new IllegalStateException("scheduler registration cleanup failed", failure);
      } finally {
        if (ownsExecutors) {
          timer.shutdownNow();
          ((ThreadPoolExecutor) cleanup).shutdown();
        }
        completeTermination();
      }
    }
  }

  private void completeTermination() {
    boolean finished;
    synchronized (this) { finished = closed && jobs.isEmpty(); }
    if (finished) {
      if (cleanupFailure == null) termination.complete(null);
      else termination.completeExceptionally(cleanupFailure);
    }
  }

  private final class Job implements ScanOperation {
    private final ScanSource source;
    private final BudgetManager.Account account;
    private final ScanRequest request;
    private final CompletableFuture<StoragePage> result = new CompletableFuture<>();
    private BudgetManager.Lease queueLease;
    private Resources resources;
    private RequestScope.ScopedCleanup registration;
    private ScheduledFuture<?> expiry;
    private ScanOperation physical;
    private Throwable startFailure;
    private boolean running;
    private boolean finished;
    private boolean stopRequested;
    private boolean disposed;

    Job(ScanSource source, BudgetManager.Account account, ScanRequest request) {
      this.source = source;
      this.account = account;
      this.request = Objects.requireNonNull(request, "request");
    }

    @Override public CompletionStage<StoragePage> completion() { return result.minimalCompletionStage(); }

    void start() {
      if (queueLease != null) queueLease.close();
      try {
        if (startFailure != null) { finish(null, startFailure); return; }
        boolean stopped;
        synchronized (ScanScheduler.this) { stopped = stopRequested; }
        if (stopped) { finish(null, cancelled()); return; }
        account.requireActive();
        ScanOperation opened = Objects.requireNonNull(source.startScan(request), "physical operation");
        boolean stop;
        synchronized (ScanScheduler.this) { physical = opened; stop = stopRequested; }
        opened.completion().whenComplete(this::finish);
        if (stop) stopPhysical(opened);
      } catch (BackendFailure | RuntimeException problem) {
        finish(null, problem);
      } catch (Error fatal) {
        finish(null, fatal);
        throw fatal;
      }
    }

    void finish(StoragePage page, Throwable failure) {
      Resources owned;
      boolean stopped;
      ScanOperation completed;
      synchronized (ScanScheduler.this) {
        if (finished) return;
        finished = true;
        stopped = stopRequested;
        owned = resources;
        completed = physical;
        physical = null;
      }
      try {
        if (completed != null) completed.close();
      } catch (RuntimeException | Error cleanupFailure) {
        var terminal = new ScanCleanupFailure(cleanupFailure);
        if (failure != null) terminal.addSuppressed(failure);
        failure = terminal;
      }
      boolean retire = hasCleanupFailure(failure);
      if (retire) synchronized (ScanScheduler.this) { cleanupFailure = failure; }
      if (owned != null) owned.finishIo(retire);
      if (failure == null && stopped) failure = cancelled();
      if (failure == null) {
        try {
          long retained = 0;
          for (var record : Objects.requireNonNull(page, "page").records()) retained = Math.addExact(retained, record.retainedBytes());
          if (retained > request.maximumBytes() || page.records().size() > request.maximumRows()) throw resourceLimit();
          account.charge(page.cost());
        } catch (BackendFailure | RuntimeException invalid) {
          failure = invalid;
        }
      }
      if (failure != null) {
        synchronized (ScanScheduler.this) { startFailure = failure; }
        close();
        result.completeExceptionally(failure);
      } else {
        result.complete(page);
      }
      pump();
    }

    void reject(Throwable failure) {
      synchronized (ScanScheduler.this) { startFailure = failure; }
      close();
      result.completeExceptionally(failure);
    }

    void expire() {
      synchronized (ScanScheduler.this) {
        if (running || finished || stopRequested) return;
        startFailure = new BackendFailure(BackendFailure.Category.TIMEOUT);
      }
      close();
    }

    @Override
    public void close() {
      ScanOperation toStop;
      boolean settle;
      synchronized (ScanScheduler.this) {
        if (disposed) return;
        boolean firstStop = !stopRequested;
        stopRequested = true;
        removeQueued(this);
        if (expiry != null) expiry.cancel(false);
        toStop = firstStop ? physical : null;
        settle = !running || finished;
        if (settle) { disposed = true; jobs.remove(this); }
      }
      try {
        if (queueLease != null) queueLease.close();
        if (toStop != null) stopPhysical(toStop);
        if (settle) {
          if (resources != null) resources.page.close();
          result.completeExceptionally(startFailure == null ? cancelled() : startFailure);
        }
      } finally {
        closeRegistration();
        completeTermination();
        pump();
      }
    }

    void closeRegistration() {
      try {
        if (registration != null) registration.close();
      } catch (Exception failure) {
        result.completeExceptionally(failure);
      }
    }

    private void stopPhysical(ScanOperation operation) {
      Runnable stop = () -> {
        try {
          operation.close();
        } catch (RuntimeException | Error failure) {
          finish(null, new ScanCleanupFailure(failure));
          if (failure instanceof Error fatal) throw fatal;
        }
      };
      try {
        cleanup.execute(stop);
      } catch (RejectedExecutionException rejected) {
        // A closing executor cannot discard ownership. Active cleanup is already ledger-bounded.
        stop.run();
      }
    }
  }

  private static final class GroupQueue {
    final Map<BudgetManager.Account, ArrayDeque<Job>> queues = new IdentityHashMap<>();
    final ArrayDeque<BudgetManager.Account> order = new ArrayDeque<>();
    void add(Job job) {
      queues.computeIfAbsent(job.account, account -> { order.add(account); return new ArrayDeque<>(); }).add(job);
    }
  }

  private record Resources(BudgetManager.Lease page, BudgetManager.Lease io, BudgetManager.Lease work) {
    void finishIo(boolean retire) {
      if (retire) { work.retire(); io.retire(); }
      else { work.close(); io.close(); }
    }
  }

  private static boolean hasCleanupFailure(Throwable failure) {
    for (int depth = 0; failure != null && depth < 16; depth++, failure = failure.getCause()) {
      if (failure instanceof ScanCleanupFailure) return true;
    }
    return false;
  }

  private static BackendFailure resourceLimit() { return new BackendFailure(BackendFailure.Category.RESOURCE_LIMIT); }
  private static BackendFailure cancelled() { return new BackendFailure(BackendFailure.Category.CANCELLED); }

  private static Duration checkedQueueWait(Duration wait) {
    if (wait.isZero() || wait.isNegative() || wait.compareTo(Duration.ofMinutes(5)) > 0) {
      throw new IllegalArgumentException("queue wait must be positive and at most five minutes");
    }
    return wait;
  }

  private static ThreadPoolExecutor cleanupExecutor(BudgetManager budget) {
    int capacity = budget.cleanupCapacity();
    return new ThreadPoolExecutor(Math.min(2, capacity), Math.min(2, capacity), 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(capacity), runnable -> {
          Thread thread = new Thread(runnable, "storage-scan-cleanup"); thread.setDaemon(true); return thread;
        });
  }

  private static ScheduledThreadPoolExecutor timerExecutor() {
    var executor = new ScheduledThreadPoolExecutor(1, runnable -> {
      Thread thread = new Thread(runnable, "storage-scan-expiry"); thread.setDaemon(true); return thread;
    });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }
}
