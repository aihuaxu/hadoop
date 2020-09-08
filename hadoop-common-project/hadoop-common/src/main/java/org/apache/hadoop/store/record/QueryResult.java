package org.apache.hadoop.store.record;

import java.util.Collections;
import java.util.List;

/**
 * Abstract from release-int-3.1.0 hdfs router
 * @param <T>
 */
public class QueryResult<T extends BaseRecord> {

  /** Data result. */
  private final List<T> records;
  /** Time stamp of the data results. */
  private final long timestamp;

  public QueryResult(final List<T> recs, final long time) {
    this.records = recs;
    this.timestamp = time;
  }

  /**
   * Get the result of the query.
   *
   * @return List of records.
   */
  public List<T> getRecords() {
    return Collections.unmodifiableList(this.records);
  }

  /**
   * The timetamp in driver time of this query.
   *
   * @return Timestamp in driver time.
   */
  public long getTimestamp() {
    return this.timestamp;
  }
}
