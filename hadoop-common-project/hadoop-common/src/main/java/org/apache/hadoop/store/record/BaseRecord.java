package org.apache.hadoop.store.record;

import org.apache.hadoop.util.Time;

import java.util.Map;

public abstract class BaseRecord implements Comparable<BaseRecord> {
  public static final String ERROR_MSG_CREATION_TIME_NEGATIVE =
      "The creation time for the record cannot be negative.";
  public static final String ERROR_MSG_MODIFICATION_TIME_NEGATIVE =
      "The modification time for the record cannot be negative.";

  /**
   * Set the modification time for the record.
   * Abstract from release-int-3.1.0 hdfs router
   * @param time Modification time of the record.
   */
  public abstract void setDateModified(long time);

  /**
   * Get the modification time for the record.
   *
   * @return Modification time of the record.
   */
  public abstract long getDateModified();

  /**
   * Set the creation time for the record.
   *
   * @param time Creation time of the record.
   */
  public abstract void setDateCreated(long time);

  /**
   * Get the creation time for the record.
   *
   * @return Creation time of the record
   */
  public abstract long getDateCreated();

  /**
   * Get the expiration time for the record.
   *
   * @return Expiration time for the record.
   */
  public abstract long getExpirationMs();

  /**
   * Map of primary key names->values for the record. The primary key can be a
   * combination of 1-n different State Store serialized values.
   *
   * @return Map of key/value pairs that constitute this object's primary key.
   */
  public abstract Map<String, String> getPrimaryKeys();

  /**
   * Initialize the object.
   */
  public void init() {
    // Call this after the object has been constructed
    initDefaultTimes();
  }

  /**
   * Initialize default times. The driver may update these timestamps on insert
   * and/or update. This should only be called when initializing an object that
   * is not backed by a data store.
   */
  private void initDefaultTimes() {
    long now = Time.now();
    this.setDateCreated(now);
    this.setDateModified(now);
  }

  /**
   * Join the primary keys into one single primary key.
   *
   * @return A string that is guaranteed to be unique amongst all records of
   * this type.
   */
  public String getPrimaryKey() {
    return generateMashupKey(getPrimaryKeys());
  }

  /**
   * Generates a cache key from a map of values.
   *
   * @param keys Map of values.
   * @return String mashup of key values.
   */
  protected static String generateMashupKey(final Map<String, String> keys) {
    StringBuilder builder = new StringBuilder();
    for (Object value : keys.values()) {
      if (builder.length() > 0) {
        builder.append("-");
      }
      builder.append(value);
    }
    return builder.toString();
  }

  /**
   * Check if this record matches a partial record.
   *
   * @param other Partial record.
   * @return If this record matches.
   */
  public boolean like(BaseRecord other) {
    if (other == null) {
      return false;
    }
    Map<String, String> thisKeys = this.getPrimaryKeys();
    Map<String, String> otherKeys = other.getPrimaryKeys();
    if (thisKeys == null) {
      return otherKeys == null;
    }
    return thisKeys.equals(otherKeys);
  }

  /**
   * Override equals check to use primary key(s) for comparison.
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BaseRecord)) {
      return false;
    }

    BaseRecord baseObject = (BaseRecord) obj;
    Map<String, String> keyset1 = this.getPrimaryKeys();
    Map<String, String> keyset2 = baseObject.getPrimaryKeys();
    return keyset1.equals(keyset2);
  }

  /**
   * Override hash code to use primary key(s) for comparison.
   */
  @Override
  public int hashCode() {
    Map<String, String> keyset = this.getPrimaryKeys();
    return keyset.hashCode();
  }

  @Override
  public int compareTo(BaseRecord record) {
    if (record == null) {
      return -1;
    }
    // Descending date order
    return (int) (record.getDateModified() - this.getDateModified());
  }

  /**
   * Called when the modification time and current time is available, checks for
   * expirations.
   *
   * @param currentTime The current timestamp in ms from the data store, to be
   *                    compared against the modification and creation dates of the
   *                    object.
   * @return boolean True if the record has been updated and should be
   * committed to the data store. Override for customized behavior.
   */
  public boolean checkExpired(long currentTime) {
    long expiration = getExpirationMs();
    if (getDateModified() > 0 && expiration > 0) {
      return (getDateModified() + expiration) < currentTime;
    }
    return false;
  }

  /**
   * Validates the record. Called when the record is created, populated from the
   * state store, and before committing to the state store. If validate failed,
   * there throws an exception.
   */
  public void validate() {
    if (getDateCreated() <= 0) {
      throw new IllegalArgumentException(ERROR_MSG_CREATION_TIME_NEGATIVE);
    } else if (getDateModified() <= 0) {
      throw new IllegalArgumentException(ERROR_MSG_MODIFICATION_TIME_NEGATIVE);
    }
  }

  @Override
  public String toString() {
    return getPrimaryKey();
  }
}
