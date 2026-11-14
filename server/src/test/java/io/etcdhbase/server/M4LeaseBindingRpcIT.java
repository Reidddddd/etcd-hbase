package io.etcdhbase.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Verifies the J41 binding and registration contract through Go RPCs and MiniCluster. */
@Timeout(1200)
class M4LeaseBindingRpcIT {
  @Test
  void realGoLeaseBindingCallsMatchNativeAndSurviveRestartAcrossLayouts() throws Exception {
    M4LeaseRpcIT.verifyGoLeaseBindingsMatchNativeAndSurviveRestartAcrossLayouts();
  }
}
