package io.etcdhbase.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class InitializeTablesCommandTest {
  @Test
  void planPrintsTheSelectedSeparateHistoryCandidateWithoutOpeningHBase() throws Exception {
    CommandResult result = runPlan("history-table.yaml");

    assertEquals(0, result.exitCode(), result.error());
    assertTrue(result.output().contains("history_layout: SEPARATE_HISTORY_TABLE"));
    assertTrue(result.output().contains("etcd_candidate:system"));
    assertTrue(result.output().contains("etcd_candidate:latest"));
    assertTrue(result.output().contains("etcd_candidate:history"));
    assertTrue(result.output().contains("configured_namespace_tables:\n"));
    assertTrue(result.output().contains("  - etcd_candidate:system"));
    assertTrue(result.output().contains("split_keys_hex: [0001,0002"));
    assertTrue(result.output().contains("No HBase connection was opened"));
    assertTrue(result.error().isEmpty());
  }

  @Test
  void planPrintsTheSelectedNativeCellVersionCandidateAndDistinctFingerprint() throws Exception {
    CommandResult separate = runPlan("history-table.yaml");
    CommandResult nativeVersions = runPlan("native-cell-versions.yaml");

    assertEquals(0, nativeVersions.exitCode(), nativeVersions.error());
    assertTrue(nativeVersions.output().contains("history_layout: NATIVE_CELL_VERSIONS"));
    assertTrue(nativeVersions.output().contains("etcd_candidate:kv"));
    assertFalse(nativeVersions.output().contains("etcd_candidate:history"));
    assertNotEquals(fingerprint(separate.output()), fingerprint(nativeVersions.output()));
  }

  @Test
  void rejectsAHistoryLayoutThatConflictsWithTheSelectedConfiguration() throws Exception {
    CommandResult result =
        run(
            "init",
            "plan",
            "--config",
            examplePath("history-table.yaml"),
            "--history-layout",
            "NATIVE_CELL_VERSIONS");

    assertEquals(2, result.exitCode());
    assertTrue(result.error().contains("conflicts"), result.error());
    assertTrue(result.output().isEmpty());
  }

  @Test
  void rejectsDuplicateOptionsAndReportsMissingConfiguration() {
    CommandResult duplicate =
        run("init", "plan", "--config", "first.yaml", "--config", "second.yaml");
    CommandResult missing = run("init", "plan", "--table-group", "local");

    assertEquals(2, duplicate.exitCode());
    assertTrue(duplicate.error().contains("duplicate option"));
    assertEquals(2, missing.exitCode());
    assertTrue(missing.error().contains("--config"));
  }

  @Test
  void rejectsAnIncorrectApplyFingerprintBeforeOpeningHBase() throws Exception {
    CommandResult result =
        run(
            "init",
            "apply",
            "--config",
            examplePath("history-table.yaml"),
            "--confirm-table-group",
            "local",
            "--confirm-plan",
            "sha256:" + "0".repeat(64));

    assertEquals(2, result.exitCode());
    assertTrue(result.error().contains("--confirm-plan does not match"), result.error());
    assertFalse(result.error().contains("HBase operation failed"), result.error());
    assertTrue(result.output().isEmpty());
  }

  @Test
  void separatesConfigurationReadErrorsFromBackendIoErrors() throws Exception {
    String configuration = examplePath("history-table.yaml");
    String confirmationFingerprint =
        fingerprint(runPlan("history-table.yaml").output())
            .substring("confirmation_fingerprint: ".length());
    CommandResult backendFailure =
        runWithApplyOperation(
            (plan, output) -> {
              throw new IOException("simulated backend failure");
            },
            "init",
            "apply",
            "--config",
            configuration,
            "--confirm-table-group",
            "local",
            "--confirm-plan",
            confirmationFingerprint);

    assertEquals(4, backendFailure.exitCode());
    assertTrue(backendFailure.error().contains("HBase operation failed (IOException)"));
    assertFalse(backendFailure.error().contains("configuration cannot be read"));

    Path missingConfiguration = Files.createTempFile("m0-admin-missing-", ".yaml");
    Files.delete(missingConfiguration);
    CommandResult configurationFailure =
        run("init", "plan", "--config", missingConfiguration.toString());

    assertEquals(2, configurationFailure.exitCode());
    assertTrue(configurationFailure.error().contains("configuration cannot be read"));
  }

  @Test
  void planOutputDoesNotContainCredentialFieldNamesOrYamlComments() throws Exception {
    CommandResult result = runPlan("history-table.yaml");

    assertEquals(0, result.exitCode(), result.error());
    assertFalse(result.output().contains("credential_reference"));
    assertFalse(result.output().contains("password"));
    assertFalse(result.output().contains("Disposable loopback"));
  }

  private static CommandResult runPlan(String configName) throws Exception {
    return run("init", "plan", "--config", examplePath(configName));
  }

  private static String examplePath(String name) throws Exception {
    try (InputStream input =
        InitializeTablesCommandTest.class.getResourceAsStream("/m0-examples/" + name)) {
      if (input == null) {
        throw new IllegalStateException("missing M0 config fixture");
      }
      Path copy = Files.createTempFile("m0-admin-", ".yaml");
      Files.write(copy, input.readAllBytes());
      copy.toFile().deleteOnExit();
      return copy.toAbsolutePath().toString();
    }
  }

  private static CommandResult run(String... args) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream error = new ByteArrayOutputStream();
    try (PrintStream outputStream = new PrintStream(output, true, StandardCharsets.UTF_8);
        PrintStream errorStream = new PrintStream(error, true, StandardCharsets.UTF_8)) {
      int exitCode = InitializeTablesCommand.run(args, outputStream, errorStream);
      return new CommandResult(
          exitCode,
          output.toString(StandardCharsets.UTF_8),
          error.toString(StandardCharsets.UTF_8));
    }
  }

  private static CommandResult runWithApplyOperation(
      InitializeTablesCommand.ApplyOperation applyOperation, String... args) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream error = new ByteArrayOutputStream();
    try (PrintStream outputStream = new PrintStream(output, true, StandardCharsets.UTF_8);
        PrintStream errorStream = new PrintStream(error, true, StandardCharsets.UTF_8)) {
      int exitCode =
          InitializeTablesCommand.run(args, outputStream, errorStream, applyOperation);
      return new CommandResult(
          exitCode,
          output.toString(StandardCharsets.UTF_8),
          error.toString(StandardCharsets.UTF_8));
    }
  }

  private static String fingerprint(String output) {
    return output.lines()
        .filter(line -> line.startsWith("confirmation_fingerprint: "))
        .findFirst()
        .orElseThrow();
  }

  private record CommandResult(int exitCode, String output, String error) {}
}
