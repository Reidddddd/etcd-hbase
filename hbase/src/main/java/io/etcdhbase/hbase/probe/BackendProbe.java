package io.etcdhbase.hbase.probe;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/** Runs bounded raw-byte operations against one explicitly provisioned probe table. */
public final class BackendProbe {
  private static final Pattern RUN_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
  private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
  private static final byte[] FAMILY = bytes("d");
  private static final byte[] QUALIFIER = bytes("probe-value");

  private BackendProbe() {}

  /**
   * Writes two binary rows, verifies Get and compare-and-mutate behavior, then scans a bounded
   * range. Any backend failure is propagated to the caller; it is never represented as an empty
   * result or a failed comparison.
   */
  public static ProbeResult execute(AsyncConnection connection, TableName tableName, String runId)
      throws Exception {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(tableName, "tableName");
    requireRunId(runId);

    ProbeRows rows = rows(runId);
    AsyncTable<?> table = connection.getTable(tableName);
    await(
        table.putAll(
            List.of(
                new Put(rows.firstKey()).addColumn(FAMILY, QUALIFIER, rows.firstValue()),
                new Put(rows.secondKey()).addColumn(FAMILY, QUALIFIER, rows.secondValue()))));

    assertValue(
        await(table.get(new Get(rows.firstKey()).addColumn(FAMILY, QUALIFIER))),
        rows.firstValue());

    Put replacement =
        new Put(rows.firstKey()).addColumn(FAMILY, QUALIFIER, rows.replacementValue());
    CheckAndMutate matchingUpdate =
        CheckAndMutate.newBuilder(rows.firstKey())
            .ifEquals(FAMILY, QUALIFIER, rows.firstValue())
            .build(replacement);
    CheckAndMutateResult successfulUpdate = await(table.checkAndMutate(matchingUpdate));
    if (!successfulUpdate.isSuccess()) {
      throw new IllegalStateException("probe compare-and-mutate rejected the current value");
    }

    Put staleReplacement =
        new Put(rows.firstKey()).addColumn(FAMILY, QUALIFIER, rows.staleReplacementValue());
    CheckAndMutate staleUpdate =
        CheckAndMutate.newBuilder(rows.firstKey())
            .ifEquals(FAMILY, QUALIFIER, rows.firstValue())
            .build(staleReplacement);
    CheckAndMutateResult rejectedUpdate = await(table.checkAndMutate(staleUpdate));
    if (rejectedUpdate.isSuccess()) {
      throw new IllegalStateException("probe compare-and-mutate accepted a stale value");
    }
    assertValue(
        await(table.get(new Get(rows.firstKey()).addColumn(FAMILY, QUALIFIER))),
        rows.replacementValue());

    List<byte[]> scannedRows = new ArrayList<>(2);
    Scan scan =
        new Scan()
            .withStartRow(rows.firstKey(), true)
            .withStopRow(rows.stopRow(), false)
            .setLimit(3)
            .addColumn(FAMILY, QUALIFIER);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result;
      while ((result = scanner.next()) != null) {
        scannedRows.add(result.getRow());
      }
    }

    if (scannedRows.size() != 2
        || !Arrays.equals(rows.firstKey(), scannedRows.get(0))
        || !Arrays.equals(rows.secondKey(), scannedRows.get(1))) {
      throw new IllegalStateException("probe scan did not return the expected ordered rows");
    }
    return new ProbeResult(true, successfulUpdate.isSuccess(), !rejectedUpdate.isSuccess(), 2);
  }

  /** Verifies that a newly created connection can read the value written by an earlier one. */
  public static void verifyPersistedValue(
      AsyncConnection connection, TableName tableName, String runId) throws Exception {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(tableName, "tableName");
    requireRunId(runId);
    ProbeRows rows = rows(runId);
    Result persisted =
        await(
        connection.getTable(tableName).get(new Get(rows.firstKey()).addColumn(FAMILY, QUALIFIER)));
    assertValue(persisted, rows.replacementValue());
  }

  private static ProbeRows rows(String runId) {
    byte[] suffix = runId.getBytes(StandardCharsets.US_ASCII);
    return new ProbeRows(
        key((byte) 1, suffix),
        key((byte) 2, suffix),
        key((byte) 3, suffix),
        new byte[] {0, (byte) 0xff, 0x11},
        new byte[] {(byte) 0xfe, 0x22, 0},
        new byte[] {0x33, 0, (byte) 0xff},
        new byte[] {0x44, (byte) 0xff, 0});
  }

  private static byte[] key(byte ordinal, byte[] suffix) {
    byte[] key = new byte[suffix.length + 3];
    key[0] = 0;
    key[1] = (byte) 0xff;
    key[2] = ordinal;
    System.arraycopy(suffix, 0, key, 3, suffix.length);
    return key;
  }

  private static void assertValue(Result result, byte[] expected) {
    if (result.isEmpty() || !Arrays.equals(expected, result.getValue(FAMILY, QUALIFIER))) {
      throw new IllegalStateException("probe read did not return the expected raw bytes");
    }
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    try {
      return future.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeout) {
      future.cancel(true);
      throw timeout;
    } catch (InterruptedException interrupted) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw interrupted;
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("probe operation failed", cause);
    }
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static void requireRunId(String runId) {
    Objects.requireNonNull(runId, "runId");
    if (!RUN_ID.matcher(runId).matches()) {
      throw new IllegalArgumentException("runId must contain only ASCII letters, digits, _ or -");
    }
  }

  private record ProbeRows(
      byte[] firstKey,
      byte[] secondKey,
      byte[] stopRow,
      byte[] firstValue,
      byte[] secondValue,
      byte[] replacementValue,
      byte[] staleReplacementValue) {}

  public record ProbeResult(
      boolean rawBytesRoundTrip,
      boolean compareAndMutateSucceeded,
      boolean staleCompareAndMutateRejected,
      int scannedRows) {}
}
