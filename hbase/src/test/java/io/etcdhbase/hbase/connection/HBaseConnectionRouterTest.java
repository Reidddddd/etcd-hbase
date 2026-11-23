package io.etcdhbase.hbase.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.hbase.api.HBaseConnectionFactory;
import io.etcdhbase.hbase.identity.HBaseBackendIdentityProvider;
import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HBaseConnectionRouterTest {
  @Test
  void closeCanBeRetriedAfterTheLastIdentityRouteDrains() {
    HBaseBackendIdentityProvider provider = mock(HBaseBackendIdentityProvider.class);
    when(provider.providerId()).thenReturn("endpoint");
    var identity = new AuthorizedBackendIdentity("endpoint", "subject", "context",
        new AuthenticationScope("group", "profile"), Set.of("group"), 1);
    var router = new HBaseConnectionRouter(mock(HBaseConnectionFactory.class));
    router.register(identity, provider);

    assertThrows(IllegalStateException.class, router::close);
    assertEquals(1, router.routedIdentityCount());

    router.unregister(identity, provider).toCompletableFuture().join();
    router.close();
    router.close();
    assertEquals(0, router.routedIdentityCount());
  }
}
