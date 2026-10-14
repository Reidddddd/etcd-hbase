package io.etcdhbase.admin;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;

/** Adapts the official asynchronous HBase APIs to the bounded initializer workflow. */
final class AsyncTableGroupAdmin implements TableGroupAdmin {
  private static final long OPERATION_TIMEOUT_SECONDS = 30;

  private final AsyncConnection connection;
  private final AsyncAdmin admin;

  AsyncTableGroupAdmin(AsyncConnection connection) {
    this.connection = connection;
    this.admin = connection.getAdmin();
  }

  @Override
  public List<String> listNamespaces() throws Exception {
    return await(admin.listNamespaces());
  }

  @Override
  public void createNamespace(String namespace) throws Exception {
    await(admin.createNamespace(NamespaceDescriptor.create(namespace).build()));
  }

  @Override
  public List<TableName> listTableNames(String namespace) throws Exception {
    return await(admin.listTableNamesByNamespace(namespace));
  }

  @Override
  public TableDescriptor getDescriptor(TableName tableName) throws Exception {
    return await(admin.getDescriptor(tableName));
  }

  @Override
  public List<RegionInfo> getRegions(TableName tableName) throws Exception {
    return await(admin.getRegions(tableName));
  }

  @Override
  public boolean isEmpty(TableName tableName) throws Exception {
    List<Result> firstRow =
        await(connection.getTable(tableName).scanAll(new Scan().setLimit(1)));
    return firstRow.isEmpty();
  }

  @Override
  public void createTable(TableDescriptor descriptor, byte[][] splitKeys) throws Exception {
    await(admin.createTable(descriptor, splitKeys));
  }

  private static <T> T await(CompletableFuture<T> future) throws Exception {
    try {
      return future.get(OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (TimeoutException timeout) {
      future.cancel(true);
      throw new IllegalStateException("HBase initialization operation timed out", timeout);
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
      throw new IllegalStateException("HBase initialization operation failed", cause);
    }
  }
}
