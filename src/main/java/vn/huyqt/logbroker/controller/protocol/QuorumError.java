package vn.huyqt.logbroker.controller.protocol;

import java.io.IOException;

/**
 * Controller reply error codes. Numbers 0 to 13 keep their broker v1 meaning; 14 and above are
 * controller-specific. The number, not the ordinal, is the wire value; see {@code
 * docs/controller-protocol-v1.md}.
 */
public enum QuorumError {
  NONE(0),
  INVALID_REQUEST(1),
  UNSUPPORTED_OPERATION(2),
  UNSUPPORTED_VERSION(3),
  UNKNOWN_TOPIC(4),
  UNKNOWN_PARTITION(5),
  TOPIC_ALREADY_EXISTS(6),
  OFFSET_OUT_OF_RANGE(7),
  BATCH_TOO_LARGE(8),
  OVERLOADED(9),
  REQUEST_TIMED_OUT(10),
  PARTITION_UNAVAILABLE(11),
  STORAGE_ERROR(12),
  BROKER_SHUTTING_DOWN(13),
  NOT_LEADER(14),
  STALE_EPOCH(15),
  CLUSTER_MISMATCH(16),
  INCONSISTENT_VOTER_SET(17),
  NODE_UNAVAILABLE(18),
  SNAPSHOT_NOT_FOUND(19),
  INCOMPATIBLE_METADATA_VERSION(20),
  BROKER_ID_IN_USE(21),
  STORAGE_ID_MISMATCH(22),
  STALE_BROKER_EPOCH(23),
  NO_ELIGIBLE_BROKER(24);
  private final short number;

  QuorumError(int number) {
    this.number = (short) number;
  }

  public short number() {
    return number;
  }

  /**
   * Maps a wire number back to its constant.
   *
   * @throws IOException if the number is not defined, so an unknown code is a decode error
   */
  public static QuorumError fromNumber(short number) throws IOException {
    for (var value : values()) if (value.number == number) return value;
    throw new IOException("Unknown quorum error");
  }
}
