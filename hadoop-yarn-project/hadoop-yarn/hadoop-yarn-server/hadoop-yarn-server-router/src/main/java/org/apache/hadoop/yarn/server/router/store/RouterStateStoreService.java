package org.apache.hadoop.yarn.server.router.store;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.store.RecordStore;
import org.apache.hadoop.store.StateStoreCache;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class RouterStateStoreService extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterStateStoreService.class);

  /** State Store configuration. */
  private Configuration conf;

  /** Identifier for the service. */
  private String identifier;

  /** Driver for the back end connection. */
  private StateStoreDriver driver;
  /** Service to maintain State Store caches. */
  private StateStoreCacheUpdateService cacheUpdater;
  /** Time the cache was last successfully updated. */
  private long cacheLastUpdateTime;
  /** List of internal caches to update. */
  private final List<StateStoreCache> cachesToUpdateInternal;

  /** Supported record stores. */
  private final Map<
      Class<? extends BaseRecord>, RecordStore<? extends BaseRecord>>
      recordStores;

  public RouterStateStoreService() {
    super(RouterStateStoreService.class.getName());
    // Records and stores supported by this implementation
    this.recordStores = new HashMap<>();

    // Caches to maintain
    this.cachesToUpdateInternal = new ArrayList<>();
  }

  /**
   * Initialize the State Store and the connection to the backend.
   *
   * @param config Configuration for the State Store.
   * @throws IOException
   */
  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;

    // Create implementation of State Store
    Class<? extends StateStoreDriver> driverClass = this.conf.getClass(
        RouterConfigKeys.ROUTER_STORE_DRIVER_CLASS,
        RouterConfigKeys.ROUTER_STORE_DRIVER_CLASS_DEFAULT,
        StateStoreDriver.class);
    this.driver = ReflectionUtils.newInstance(driverClass, this.conf);

    if (this.driver == null) {
      throw new IOException("Cannot create driver for the State Store");
    }

    // Add supported record stores
    // Router record store
    addRecordStore(RouterRecordStoreImpl.class);

    RouterState.setExpirationMs(conf.getTimeDuration(
        RouterConfigKeys.ROUTER_STORE_ROUTER_EXPIRATION_MS,
        RouterConfigKeys.ROUTER_STORE_ROUTER_EXPIRATION_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    // Cache update service
    this.cacheUpdater = new StateStoreCacheUpdateService(this);
    addService(this.cacheUpdater);

    super.serviceInit(this.conf);
  }
  @Override
  protected void serviceStart() throws Exception {
    loadDriver();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeDriver();
    super.serviceStop();
  }

  /**
   * Get the state store driver.
   *
   * @return State store driver.
   */
  public StateStoreDriver getDriver() {
    return this.driver;
  }

  /**
   * Manually shuts down the driver.
   *
   * @throws Exception If the driver cannot be closed.
   */
  @VisibleForTesting
  public void closeDriver() throws Exception {
    if (this.driver != null) {
      this.driver.close();
    }
  }

  /**
   * Add a record store to the State Store. It includes adding the store, the
   * supported record and the cache management.
   *
   * @param clazz Class of the record store to track.
   * @return New record store.
   * @throws ReflectiveOperationException
   */
  private <T extends RecordStore<?>> void addRecordStore(
      final Class<T> clazz) throws ReflectiveOperationException {

    assert this.getServiceState() == STATE.INITED :
        "Cannot add record to the State Store once started";

    T recordStore = RecordStore.newInstance(clazz, this.getDriver());
    Class<? extends BaseRecord> recordClass = recordStore.getRecordClass();
    this.recordStores.put(recordClass, recordStore);

    // Subscribe for cache updates
    if (recordStore instanceof StateStoreCache) {
      StateStoreCache cachedRecordStore = (StateStoreCache) recordStore;
      this.cachesToUpdateInternal.add(cachedRecordStore);
    }
  }

  /**
   * Check if the driver is ready to be used.
   *
   * @return If the driver is ready.
   */
  public boolean isDriverReady() {
    return this.driver.isDriverReady();
  }

  /**
   * Set a unique synchronization identifier for this store.
   *
   * @param id Unique identifier, typically the router's RPC address.
   */
  public void setIdentifier(String id) {
    this.identifier = id;
  }

  /**
   * Fetch a unique identifier for this state store instance. Typically it is
   * the address of the router.
   *
   * @return Unique identifier for this store.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * List of records supported by this State Store.
   *
   * @return List of supported record classes.
   */
  public Collection<Class<? extends BaseRecord>> getSupportedRecords() {
    return this.recordStores.keySet();
  }

  /**
   * Refresh the cache with information from the State Store. Called
   * periodically by the CacheUpdateService to maintain data caches and
   * versions.
   */
  public void refreshCaches() {
    refreshCaches(false);
  }

  /**
   * Refresh the cache with information from the State Store. Called
   * periodically by the CacheUpdateService to maintain data caches and
   * versions.
   * @param force If we force the refresh.
   */
  public void refreshCaches(boolean force) {
    boolean success = true;
    if (isDriverReady()) {
      List<StateStoreCache> cachesToUpdate = new LinkedList<>();
      cachesToUpdate.addAll(cachesToUpdateInternal);
      for (StateStoreCache cachedStore : cachesToUpdate) {
        String cacheName = cachedStore.getClass().getSimpleName();
        boolean result = false;
        try {
          result = cachedStore.loadCache(force);
        } catch (IOException e) {
          LOG.error("Error updating cache for {}", cacheName, e);
          result = false;
        }
        if (!result) {
          success = false;
          LOG.error("Cache update failed for cache {}", cacheName);
        }
      }
    } else {
      success = false;
      LOG.info("Skipping State Store cache update, driver is not ready.");
    }
    if (success) {
      // Uses local time, not driver time.
      this.cacheLastUpdateTime = Time.now();
    }
  }

  /**
   * Load the State Store driver. If successful, refresh cached data tables.
   */
  public void loadDriver() {
    synchronized (this.driver) {
      if (!isDriverReady()) {
        String driverName = this.driver.getClass().getSimpleName();
        if (this.driver.init(
            conf, getIdentifier(), getSupportedRecords())) {
          LOG.info("Connection to the State Store driver {} is open and ready", driverName);
          this.refreshCaches();
        } else {
          LOG.error("Cannot initialize State Store driver {}", driverName);
        }
      }
    }
  }

  /**
   * The last time the state store cache was fully updated.
   *
   * @return Timestamp.
   */
  public long getCacheUpdateTime() {
    return this.cacheLastUpdateTime;
  }

  /**
   * Get the record store in this State Store for a given interface.
   *
   * @param recordStoreClass Class of the record store.
   * @return Registered record store or null if not found.
   */
  public <T extends RecordStore<?>> T getRegisteredRecordStore(
      final Class<T> recordStoreClass) {
    for (RecordStore<? extends BaseRecord> recordStore :
        this.recordStores.values()) {
      if (recordStoreClass.isInstance(recordStore)) {
        @SuppressWarnings("unchecked")
        T recordStoreChecked = (T) recordStore;
        return recordStoreChecked;
      }
    }
    return null;
  }
}
