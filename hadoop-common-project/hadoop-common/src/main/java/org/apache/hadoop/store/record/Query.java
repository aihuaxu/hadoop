package org.apache.hadoop.store.record;

/**
 * Check if a record matches a query. The query is usually a partial record.
 * Abstract from release-int-3.1.0 hdfs router
 *
 * @param <T> Type of the record to query.
 */
public class Query<T extends BaseRecord> {

  /** Partial object to compare against. */
  private final T partial;


  /**
   * Create a query to search for a partial record.
   *
   * @param part It defines the attributes to search.
   */
  public Query(final T part) {
    this.partial = part;
  }

  /**
   * Get the partial record used to query.
   *
   * @return The partial record used for the query.
   */
  public T getPartial() {
    return this.partial;
  }

  /**
   * Check if a record matches the primary keys or the partial record.
   *
   * @param other Record to check.
   * @return If the record matches. Don't match if there is no partial.
   */
  public boolean matches(T other) {
    if (this.partial == null) {
      return false;
    }
    return this.partial.like(other);
  }

  @Override
  public String toString() {
    return "Checking: " + this.partial;
  }
}
