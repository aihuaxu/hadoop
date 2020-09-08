package org.apache.hadoop.store.driver.impl;

import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.Query;
import org.apache.hadoop.store.record.QueryResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract from release-int-3.1.0 hdfs router
 */
public abstract class StateStoreBaseImpl extends StateStoreDriver {

  @Override
  public <T extends BaseRecord> T get(
      Class<T> clazz, Query<T> query) throws IOException {
    List<T> records = getMultiple(clazz, query);
    if (records.size() > 1) {
      throw new IOException("Found more than one object in collection");
    } else if (records.size() == 1) {
      return records.get(0);
    } else {
      return null;
    }
  }

  @Override
  public <T extends BaseRecord> List<T> getMultiple(
      Class<T> clazz, Query<T> query) throws IOException  {
    QueryResult<T> result = get(clazz);
    List<T> records = result.getRecords();
    List<T> ret = filterMultiple(query, records);
    if (ret == null) {
      throw new IOException("Cannot fetch records from the store");
    }
    return ret;
  }

  @Override
  public <T extends BaseRecord> boolean put(
      T record, boolean allowUpdate, boolean errorIfExists) throws IOException {
    List<T> singletonList = new ArrayList<>();
    singletonList.add(record);
    return putAll(singletonList, allowUpdate, errorIfExists);
  }

  @Override
  public <T extends BaseRecord> boolean remove(T record) throws IOException {
    final Query<T> query = new Query<T>(record);
    Class<? extends BaseRecord> clazz = record.getClass();
    @SuppressWarnings("unchecked")
    Class<T> recordClass = (Class<T>) getRecordClass(clazz);
    return remove(recordClass, query) == 1;
  }

  /**
   * Get the base class for a record class. If we get an implementation of a
   * record we will return the real parent record class.
   *
   * @param clazz Class of the data record to check.
   * @return Base class for the record.
   */
  @SuppressWarnings("unchecked")
  private <T extends BaseRecord>
  Class<? extends BaseRecord> getRecordClass(final Class<T> clazz) {

    // We ignore the Impl classes and go to the super class
    Class<? extends BaseRecord> actualClazz = clazz;
    while (actualClazz.getSimpleName().endsWith("Impl")) {
      actualClazz = (Class<? extends BaseRecord>) actualClazz.getSuperclass();
    }

    // Check if we went too far
    if (actualClazz.equals(BaseRecord.class)) {
      actualClazz = clazz;
    }
    return actualClazz;
  }

  /**
   * Filters a list of records to find all records matching the query.
   *
   * @param query Map of field names and objects to use to filter results.
   * @param records List of data records to filter.
   * @return List of all records matching the query (or empty list if none
   *         match), null if the data set could not be filtered.
   */
  private <T extends BaseRecord> List<T> filterMultiple(
      final Query<T> query, final Iterable<T> records) {

    List<T> matchingList = new ArrayList<>();
    for (T record : records) {
      if (query.matches(record)) {
        matchingList.add(record);
      }
    }
    return matchingList;
  }
}