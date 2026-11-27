package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.HistoryGcPermit;
import io.etcdhbase.core.request.RequestScope;
import io.etcdhbase.hbase.storage.HBaseHistoryCollector;
import io.etcdhbase.hbase.storage.HBaseHistoryKeyEnumerator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PhysicalHistoryCollectionTest {
  @Test
  void drainsEveryVersionPageBeforeReportingAKeyComplete() {
    var enumerator = mock(HBaseHistoryKeyEnumerator.class);
    var collector = mock(HBaseHistoryCollector.class);
    var key = ByteKey.copyOf(new byte[] {1});
    when(enumerator.nextKeys(any(), any(), anyInt(), any())).thenReturn(
        CompletableFuture.completedFuture(new HBaseHistoryKeyEnumerator.Page(
            List.of(key), Optional.of(key), true)));
    when(collector.collectKey(any(), any(), anyLong(), anyInt(), any())).thenReturn(
        CompletableFuture.completedFuture(new HBaseHistoryCollector.Page(64, false, 64)),
        CompletableFuture.completedFuture(new HBaseHistoryCollector.Page(128, false, 64)),
        CompletableFuture.completedFuture(new HBaseHistoryCollector.Page(130, true, 2)));
    try (var scope = RequestScope.withoutDeadline()) {
      assertEquals(130, new PhysicalHistoryCollection(enumerator, collector)
          .collect(mock(HistoryGcPermit.class), scope).join());
    }
  }
}
