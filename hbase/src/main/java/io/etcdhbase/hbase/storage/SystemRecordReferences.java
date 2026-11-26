package io.etcdhbase.hbase.storage;

import io.etcdhbase.core.bytes.ByteKey;
import io.etcdhbase.core.commit.ManifestRecordFormat;
import io.etcdhbase.core.commit.ResultDirectory;
import io.etcdhbase.core.commit.ResultPage;
import io.etcdhbase.core.commit.SystemRecordEntry;
import io.etcdhbase.core.commit.SystemRecordType;
import io.etcdhbase.hbase.codec.ManifestMessages;
import io.etcdhbase.hbase.codec.ResultMessages;

/** Decodes the direct edges of immutable manifest and result records, checking their row identity. */
final class SystemRecordReferences {
  private SystemRecordReferences() {}

  static boolean manifestReferences(SystemRecordEntry entry, SystemRecordType type, ByteKey target) {
    byte[] identity = entry.identity().bytes();
    if (identity.length < 17) throw new IllegalArgumentException("invalid manifest identity");
    byte[] value = entry.value().bytes();
    return switch (identity[16]) {
      case ManifestRecordFormat.MANIFEST_ROOT -> rootReferences(entry.identity(), value, type, target);
      case ManifestRecordFormat.MANIFEST_PAGE -> {
        var page = ManifestMessages.decodePage(value);
        requireIdentity(entry.identity(), page.systemIdentity());
        yield type == SystemRecordType.CANDIDATE && page.candidates().stream()
            .anyMatch(candidate -> candidate.candidateIdentity().equals(target));
      }
      case ManifestRecordFormat.MANIFEST_DIRECTORY_PAGE -> {
        var directory = ManifestMessages.decodeDirectory(value);
        requireIdentity(entry.identity(), directory.systemIdentity());
        yield type == SystemRecordType.MANIFEST && directory.children().stream()
            .anyMatch(child -> child.identity().equals(target));
      }
      case ManifestRecordFormat.EVENT_PAGE -> {
        var page = ManifestMessages.decodeEventPage(value);
        requireIdentity(entry.identity(), page.systemIdentity());
        yield type == SystemRecordType.EVENT && page.events().stream()
            .anyMatch(event -> event.identity().equals(target));
      }
      case ManifestRecordFormat.EVENT_DIRECTORY_PAGE -> {
        var directory = ManifestMessages.decodeEventDirectory(value);
        requireIdentity(entry.identity(), directory.systemIdentity());
        yield type == SystemRecordType.MANIFEST && directory.children().stream()
            .anyMatch(child -> child.identity().equals(target));
      }
      default -> throw new IllegalArgumentException("unknown manifest record kind");
    };
  }

  private static boolean rootReferences(ByteKey identity, byte[] value,
      SystemRecordType type, ByteKey target) {
    var root = ManifestMessages.decodeRoot(value);
    requireIdentity(identity, root.systemIdentity());
    return switch (type) {
      case MANIFEST -> root.candidateDirectory().filter(child -> child.identity().equals(target)).isPresent()
          || root.eventDirectory().filter(child -> child.identity().equals(target)).isPresent();
      case RESULT -> root.resultReference()
          .filter(result -> ResultDirectory.systemIdentity(result.identity()).equals(target)).isPresent();
      case AUXILIARY -> root.auxiliaryReference().filter(child -> child.identity().equals(target)).isPresent();
      default -> false;
    };
  }

  static boolean resultReferences(SystemRecordEntry entry, ByteKey target) {
    byte[] identity = entry.identity().bytes();
    if (identity.length < 17) throw new IllegalArgumentException("invalid result identity");
    return switch (identity[16]) {
      case ManifestRecordFormat.RESULT_DIRECTORY -> {
        var directory = ResultMessages.decodeDirectory(entry.value().bytes());
        requireIdentity(entry.identity(), directory.systemIdentity());
        yield directory.pages().stream()
            .anyMatch(page -> ResultPage.systemIdentity(page.identity(), page.index()).equals(target));
      }
      case ManifestRecordFormat.RESULT_PAGE -> {
        var page = ResultMessages.decodePage(entry.value().bytes());
        requireIdentity(entry.identity(), page.systemIdentity());
        yield false;
      }
      default -> throw new IllegalArgumentException("unknown result record kind");
    };
  }

  static void requireIdentity(ByteKey actual, ByteKey canonical) {
    if (!actual.equals(canonical)) throw new IllegalArgumentException("system record identity mismatch");
  }
}
