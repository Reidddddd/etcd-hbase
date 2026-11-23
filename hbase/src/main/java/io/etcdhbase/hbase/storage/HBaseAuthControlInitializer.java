package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.connection.HBaseRequestContext;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Creates one new auth-control table and verifies its persisted descriptor before use. */
public final class HBaseAuthControlInitializer {
  private HBaseAuthControlInitializer() {}

  /**
   * Fresh-only bootstrap using an independently authorized operator identity. Existing tables are
   * never adopted, rewritten, or deleted; an uncertain create result must be inspected by the
   * caller before retrying this operation.
   */
  public static CompletableFuture<Void> initializeFresh(
      HBaseConnectionFactory connections, AuthorizedBackendIdentity operatorIdentity,
      AuthControlBinding binding, RequestScope scope) {
    Objects.requireNonNull(connections, "connections");
    Objects.requireNonNull(operatorIdentity, "operatorIdentity");
    Objects.requireNonNull(binding, "binding");
    Objects.requireNonNull(scope, "scope");
    var request = new HBaseRequestContext(scope);
    return request.borrowConnection(connections, operatorIdentity).thenCompose(connection -> {
      var admin = connection.getAdmin();
      return HBaseRequestCompletion.startRequest(scope,
          () -> admin.tableExists(binding.table())).thenCompose(exists -> {
            if (exists) {
              return CompletableFuture.failedFuture(new IllegalStateException(
                  "auth-control table already exists; fresh initialization was refused"));
            }
            byte[][] splits = binding.splitKeys().toArray(byte[][]::new);
            return HBaseRequestCompletion.startRequest(scope,
                () -> admin.createTable(binding.descriptor(), splits))
                .thenCompose(ignored -> HBaseRequestCompletion.startRequest(scope,
                    () -> admin.getDescriptor(binding.table())))
                .thenAccept(binding::validate);
          });
    });
  }
}
