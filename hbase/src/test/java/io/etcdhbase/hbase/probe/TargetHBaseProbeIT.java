package io.etcdhbase.hbase.probe;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

/** Target acceptance gate; it fails until B02, B03, and B06 are supplied and wired. */
class TargetHBaseProbeIT {
  @Test
  void reportsMissingTargetInputsInsteadOfSkipping() throws Exception {
    String configReference = System.getProperty("m0.hbase.target.configRef", "").trim();
    if (configReference.isEmpty()) {
      fail("missing-input: B02/B03/B06; set ETCD_HBASE_M0_TARGET_CONFIG to a controlled config ref");
    }

    Path configPath = Path.of(configReference);
    if (!Files.isRegularFile(configPath) || !Files.isReadable(configPath)) {
      fail("missing-input: target config reference is not a readable local file");
    }

    fail(
        "missing-input: B02 target client/JDK combination, B03 authorized identity provider and "
            + "credentials reference, and B06 TLS/permission profile are not yet configured; "
            + "this build only contains the public HBase candidate and DEV_NO_AUTH provider");
  }
}
