package io.etcdhbase.core.commit;

/** Stable tag assignments embedded in the canonical identities of J22 manifest records. */
public final class ManifestRecordFormat {
  public static final byte MANIFEST_PAGE = 1;
  public static final byte MANIFEST_DIRECTORY_PAGE = 2;
  public static final byte MANIFEST_ROOT = 3;
  public static final byte PREPARED_EVENT = 4;
  public static final byte EVENT_PAGE = 5;
  public static final byte EVENT_DIRECTORY_PAGE = 6;
  public static final byte RESULT_PAGE = 7;
  public static final byte RESULT_DIRECTORY = 8;
  public static final byte AUXILIARY_RECORD = 9;
  public static final byte AUXILIARY_PAGE = 10;
  public static final byte AUXILIARY_DIRECTORY = 11;

  private ManifestRecordFormat() {}
}
