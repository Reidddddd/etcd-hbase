package io.etcdhbase.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.hadoop.hbase.HBaseConfiguration;

/** Actual runtime process used to prove that GC resumes without the previous JVM's memory. */
public final class J52PhysicalRuntimeProcess {
  private J52PhysicalRuntimeProcess() {}

  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 6) throw new IllegalArgumentException("invalid physical runtime fixture arguments");
    var configuration = HBaseConfiguration.create();
    configuration.addResource(new org.apache.hadoop.fs.Path(Path.of(arguments[0]).toUri()));
    try (var fixture = J52PhysicalCompactIT.Fixture.openExisting(configuration, arguments[1],
        Integer.parseInt(arguments[2]), arguments[3], arguments[4])) {
      var ready = Path.of(arguments[5]);
      var staging = ready.resolveSibling(ready.getFileName() + ".tmp");
      Files.writeString(staging, Integer.toString(fixture.port()));
      Files.move(staging, ready, StandardCopyOption.ATOMIC_MOVE);
      // EOF/newline requests a graceful drain. The failure-injection case forcibly kills the JVM.
      System.in.read();
    }
  }
}
