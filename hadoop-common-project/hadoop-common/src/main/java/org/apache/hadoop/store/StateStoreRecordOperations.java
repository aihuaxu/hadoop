package org.apache.hadoop.store;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.Query;
import org.apache.hadoop.store.record.QueryResult;

import java.io.IOException;
import java.util.List;

/**
 * Operations for a driver to manage records in the State Store.
 * Abstract from release-int-3.1.0 hdfs router
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface StateStoreRecordOperations {

  /**
   * Get all records of the requested record class from the data store. To use
   * the default implementations in this class, getAll must return new instances
   * of the records on each call. It is recommended to override the default
   * implementations for better performance.
   *
   * @param clazz Class of record to fetch.
   * @return List of all records that match the clazz.
   * @throws IOException Throws exception if unable to query the data store.
   */
  @Idempotent
  <T extends BaseRecord> QueryResult<T> get(Class<T> clazz) throws IOException;

  /**
   * Get a single record from the store that matches the query.
   *
   * @param clazz Class of record to fetch.
   * @param query Query to filter results.
   * @return A single record matching the query. Null if there are no matching
   *         records or more than one matching record in the store.
   * @throws IOException If multiple records match or if the data store cannot
   *           be queried.
   */
  @Idempotent
  <T extends BaseRecord> T get(Class<T> clazz, Query<T> query)
      throws IOException;

  /**
   * Get multiple records from the store that match a query. This method
   * assumes the underlying driver does not support filtering. If the driver
   * supports filtering it should overwrite this method.
   *
   * @param clazz Class of record to fetch.
   * @param query Query to filter results.
   * @return Records of type clazz that match the query or empty list if none
   *         are found.
   * @throws IOException Throws exception if unable to query the data store.
   */
  @Idempotent
  <T extends BaseRecord> List<T> getMultiple(
      Class<T> clazz, Query<T> query) throws IOException;

  /**
   * Creates a single record. Optionally updates an existing record with same
   * primary key.
   *
   * @param record The record to insert or update.
   * @param allowUpdate True if update of exiting record is allowed.
   * @param errorIfExists True if an error should be returned when inserting
   *          an existing record. Only used if allowUpdate = false.
   * @return True if the operation was successful.
   *
   * @throws IOException Throws exception if unable to query the data store.
   */
  @AtMostOnce
  <T extends BaseRecord> boolean put(
      T record, boolean allowUpdate, boolean errorIfExists) throws IOException;

  /**
   * Creates multiple records. Optionally updates existing records that have
   * the same primary key.
   *
   * @param records List of data records to update or create. All records must
   *                be of class clazz.
   * @param allowUpdate True if update of exiting record is allowed.
   * @param errorIfExists True if an error should be returned when inserting
   *          an existing record. Only used if allowUpdate = false.
   * @return true if all operations were successful.
   *
   * @throws IOException Throws exception if unable to query the data store.
   */
  @AtMostOnce
  <T extends BaseRecord> boolean putAll(
      List<T> records, boolean allowUpdate, boolean errorIfExists)
      throws IOException;

  /**
   * Remove a single record.
   *
   * @param record Record to be removed.
   * @return true If the record was successfully removed. False if the record
   *              could not be removed or not stored.
   * @throws IOException Throws exception if unable to query the data store.
   */
  @AtMostOnce
  <T extends BaseRecord> boolean remove(T record) throws IOException;

  /**
   * Remove all records of this class from the store.
   *
   * @param clazz Class of records to remove.
   * @return True if successful.
   * @throws IOException Throws exception if unable to query the data store.
   */
  @AtMostOnce
  <T extends BaseRecord> boolean removeAll(Class<T> clazz) throws IOException;

  /**
   * Remove multiple records of a specific class that match a query. Requires
   * the getAll implementation to fetch fresh records on each call.
   *
   * @param query Query to filter what to remove.
   * @return The number of records removed.
   * @throws IOException Throws exception if unable to query the data store.
   */
  @AtMostOnce
  <T extends BaseRecord> int remove(Class<T> clazz, Query<T> query)
      throws IOException;

}