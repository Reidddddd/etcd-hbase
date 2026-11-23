package io.etcdhbase.server.commit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.etcdhbase.core.commit.OwnerForwarding;
import io.etcdhbase.core.error.BackendFailure;
import org.junit.jupiter.api.Test;

class OwnerForwardingProtocolMapperTest {
  @Test
  void roundTripsInvalidTokenFailureWithoutDowngradingItsCategory() {
    OwnerForwarding.Reply expected =
        new OwnerForwarding.Unknown(BackendFailure.Category.INVALID_AUTH_TOKEN);

    OwnerForwarding.Reply actual = OwnerForwardingProtocolMapper.fromWire(
        OwnerForwardingProtocolMapper.toWire(expected));

    assertEquals(expected, actual);
  }
}
