package io.etcdhbase.hbase.api;

import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.concurrent.CompletionStage;
import org.apache.hadoop.hbase.client.AsyncConnection;

/** Manages shared HBase connections for authorized backend identities. */
public interface HBaseConnectionFactory extends AutoCloseable {
  /**
   * Borrows a manager-owned connection. Callers must not close it; only this factory closes shared
   * connections during factory shutdown.
   */
  CompletionStage<AsyncConnection> connectionFor(AuthorizedBackendIdentity identity);

  @Override
  void close();
}
