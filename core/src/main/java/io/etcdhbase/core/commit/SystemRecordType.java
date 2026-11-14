package io.etcdhbase.core.commit;

/** Stable logical record domains used by the internal M2 persistence port. */
public enum SystemRecordType {
  CONTROL(1),
  INVOCATION(2),
  MANIFEST(3),
  CANDIDATE(4),
  EVENT(5),
  DECISION(6),
  MATERIALIZATION(7),
  ROW_FENCE(8),
  SNAPSHOT_PIN(9),
  RECOVERY_CHECKPOINT(10),
  RESULT(11),
  TABLE_GROUP_IDENTITY(12),
  WORKSET(13),
  LEASE_STATE(14),
  LEASE_ENUMERATION(15),
  EXPIRY_CANDIDATE(16),
  AUXILIARY(17),
  LEASE_RESULT(18),
  LEASE_BINDING(19);

  private final int id;

  SystemRecordType(int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }

  public static SystemRecordType fromId(int id) {
    for (SystemRecordType type : values()) {
      if (type.id == id) return type;
    }
    throw new IllegalArgumentException("unknown system record type");
  }
}
