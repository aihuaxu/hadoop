package org.apache.hadoop.yarn.server.router.store.driver.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.driver.impl.StateStoreBaseImpl;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.Query;
import org.apache.hadoop.store.record.QueryResult;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;
import static org.apache.hadoop.yarn.server.router.RouterServerUtil.filterMultiple;
import static org.apache.hadoop.yarn.server.router.RouterServerUtil.getRecordName;

/**
 * {@link org.apache.hadoop.store.driver.StateStoreDriver} driver implementation that uses ZooKeeper as a
 * backend.
 * Similar implementation as HDFS router 3.1.0
 */
public class StateStoreZooKeeperImpl extends StateStoreBaseImpl {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreZooKeeperImpl.class);


  /** Configuration keys. */
  public static final String ROUTER_STORE_ZK_DRIVER_PREFIX =
      RouterConfigKeys.ROUTER_PREFIX + "driver.zk.";
  public static final String ROUTER_STORE_ZK_PARENT_PATH =
      ROUTER_STORE_ZK_DRIVER_PREFIX + "parent-path";
  public static final String ROUTER_STORE_ZK_PARENT_PATH_DEFAULT =
      "/yarn-router";


  /** Directory to store the state store data. */
  private String baseZNode;

  /** Interface to ZooKeeper. */
  private ZKCuratorManager zkManager;

  /** Mark for slashes in path names. */
  protected static final String SLASH_MARK = "0SLASH0";
  /** Mark for colon in path names. */
  protected static final String COLON_MARK = "_";

  /** Default serializer for this driver. */
  private StateStoreSerializer serializer;


  @Override
  public boolean init(final Configuration config, final String id,
      final Collection<Class<? extends BaseRecord>> records) {
    boolean ret = super.init(config, id, records);

    this.serializer = StateStoreSerializer.getSerializer(config);

    return ret;
  }

  @Override
  public String getRecordName(Class<? extends BaseRecord> cls) {
    return RouterServerUtil.getRecordName(cls);
  }

  /**
   * Serialize a record using the serializer.
   * @param record Record to serialize.
   * @return Byte array with the serialization of the record.
   */
  protected <T extends BaseRecord> byte[] serialize(T record) {
    return serializer.serialize(record);
  }

  /**
   * Serialize a record using the serializer.
   * @param record Record to serialize.
   * @return String with the serialization of the record.
   */
  protected <T extends BaseRecord> String serializeString(T record) {
    return serializer.serializeString(record);
  }

  /**
   * Creates a record from an input data string.
   * @param data Serialized text of the record.
   * @param clazz Record class.
   * @param includeDates If dateModified and dateCreated are serialized.
   * @return The created record.
   * @throws IOException
   */
  protected <T extends BaseRecord> T newRecord(
      String data, Class<T> clazz, boolean includeDates) throws IOException {
    return serializer.deserialize(data, clazz);
  }

  /**
   * Get the primary key for a record. If we don't want to store in folders, we
   * need to remove / from the name.
   *
   * @param record Record to get the primary key for.
   * @return Primary key for the record.
   */
  protected static String getPrimaryKey(BaseRecord record) {
    String primaryKey = record.getPrimaryKey();
    primaryKey = primaryKey.replaceAll("/", SLASH_MARK);
    primaryKey = primaryKey.replaceAll(":", COLON_MARK);
    return primaryKey;
  }
  @Override
  public boolean initDriver() {
    LOG.info("Initializing ZooKeeper connection");

    Configuration conf = getConf();
    baseZNode = conf.get(
        ROUTER_STORE_ZK_PARENT_PATH,
        ROUTER_STORE_ZK_PARENT_PATH_DEFAULT);
    try {
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start();
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
      return false;
    }
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(
      String className, Class<T> clazz) {
    try {
      String checkPath = getNodePath(baseZNode, className);
      zkManager.createRootDirRecursively(checkPath);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot initialize ZK node for {}: {}",
          className, e.getMessage());
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    if (zkManager  != null) {
      zkManager.close();
    }
  }

  @Override
  public boolean isDriverReady() {
    if (zkManager == null) {
      return false;
    }
    CuratorFramework curator = zkManager.getCurator();
    if (curator == null) {
      return false;
    }
    return curator.getState() == CuratorFrameworkState.STARTED;
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> get(Class<T> clazz)
      throws IOException {
    verifyDriverReady();
    long start = monotonicNow();
    List<T> ret = new ArrayList<>();
    String znode = getZNodeForClass(clazz);
    try {
      List<String> children = zkManager.getChildren(znode);
      for (String child : children) {
        try {
          String path = getNodePath(znode, child);
          Stat stat = new Stat();
          String data = zkManager.getStringData(path, stat);
          boolean corrupted = false;
          if (data == null || data.equals("")) {
            // All records should have data, otherwise this is corrupted
            corrupted = true;
          } else {
            try {
              T record = createRecord(data, stat, clazz);
              ret.add(record);
            } catch (IOException e) {
              LOG.error("Cannot create record type \"{}\" from \"{}\": {}",
                  clazz.getSimpleName(), data, e.getMessage());
              corrupted = true;
            }
          }

          if (corrupted) {
            LOG.error("Cannot get data for {} at {}, cleaning corrupted data",
                child, path);
            zkManager.delete(path);
          }
        } catch (Exception e) {
          LOG.error("Cannot get data for {}: {}", child, e.getMessage());
        }
      }
    } catch (Exception e) {
      String msg = "Cannot get children for \"" + znode + "\": " +
          e.getMessage();
      LOG.error(msg);
      throw new IOException(msg);
    }
    long end = monotonicNow();
    return new QueryResult<T>(ret, getTime());
  }

  @Override
  public <T extends BaseRecord> boolean putAll(
      List<T> records, boolean update, boolean error) throws IOException {
    verifyDriverReady();
    if (records.isEmpty()) {
      return true;
    }

    // All records should be the same
    T record0 = records.get(0);
    Class<? extends BaseRecord> recordClass = record0.getClass();
    String znode = getZNodeForClass(recordClass);

    boolean status = true;
    for (T record : records) {
      String primaryKey = getPrimaryKey(record);
      String recordZNode = getNodePath(znode, primaryKey);
      byte[] data = serialize(record);
      boolean isWritable = writeNode(recordZNode, data, update, error);
      if (!isWritable){
        status = false;
      }
    }
    return status;
  }

  @Override
  public <T extends BaseRecord> int remove(
      Class<T> clazz, Query<T> query) throws IOException {
    verifyDriverReady();
    if (query == null) {
      return 0;
    }

    // Read the current data
    long start = monotonicNow();
    List<T> records = null;
    try {
      QueryResult<T> result = get(clazz);
      records = result.getRecords();
    } catch (IOException ex) {
      LOG.error("Cannot get existing records", ex);
//      getMetrics().addFailure(monotonicNow() - start);
      return 0;
    }

    // Check the records to remove
    String znode = getZNodeForClass(clazz);
    List<T> recordsToRemove = filterMultiple(query, records);

    // Remove the records
    int removed = 0;
    for (T existingRecord : recordsToRemove) {
      LOG.info("Removing \"{}\"", existingRecord);
      try {
        String primaryKey = getPrimaryKey(existingRecord);
        String path = getNodePath(znode, primaryKey);
        if (zkManager.delete(path)) {
          removed++;
        } else {
          LOG.error("Did not remove \"{}\"", existingRecord);
        }
      } catch (Exception e) {
        LOG.error("Cannot remove \"{}\"", existingRecord, e);
//        getMetrics().addFailure(monotonicNow() - start);
      }
    }
    long end = monotonicNow();
    if (removed > 0) {
//      getMetrics().addRemove(end - start);
    }
    return removed;
  }

  @Override
  public <T extends BaseRecord> boolean removeAll(Class<T> clazz)
      throws IOException {
    long start = monotonicNow();
    boolean status = true;
    String znode = getZNodeForClass(clazz);
    LOG.info("Deleting all children under {}", znode);
    try {
      List<String> children = zkManager.getChildren(znode);
      for (String child : children) {
        String path = getNodePath(znode, child);
        LOG.info("Deleting {}", path);
        zkManager.delete(path);
      }
    } catch (Exception e) {
      LOG.error("Cannot remove {}: {}", znode, e.getMessage());
      status = false;
    }
    long time = monotonicNow() - start;
    if (status) {
//      getMetrics().addRemove(time);
    } else {
//      getMetrics().addFailure(time);
    }
    return status;
  }

  private boolean writeNode(
      String znode, byte[] bytes, boolean update, boolean error) {
    try {
      boolean created = zkManager.create(znode);
      if (!update && !created && error) {
        LOG.info("Cannot write record \"{}\", it already exists", znode);
        return false;
      }

      // Write data
      zkManager.setData(znode, bytes, -1);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot write record \"{}\": {}", znode, e.getMessage());
    }
    return false;
  }

  /**
   * Get the ZNode for a class.
   *
   * @param clazz Record class to evaluate.
   * @return The ZNode for the class.
   */
  private <T extends BaseRecord> String getZNodeForClass(Class<T> clazz) {
    String className = getRecordName(clazz);
    return getNodePath(baseZNode, className);
  }

  /**
   * Creates a record from a string returned by ZooKeeper.
   *
   * @param data The data to write.
   * @param stat Stat of the data record to create.
   * @param clazz The data record type to create.
   * @return The created record.
   * @throws IOException
   */
  private <T extends BaseRecord> T createRecord(
      String data, Stat stat, Class<T> clazz) throws IOException {
    T record = newRecord(data, clazz, false);
    record.setDateCreated(stat.getCtime());
    record.setDateModified(stat.getMtime());
    return record;
  }
}

