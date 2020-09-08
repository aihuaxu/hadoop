package org.apache.hadoop.store;

import java.io.IOException;

/**
 * Interface for a cached copy of the State Store.
 * Abstract from release-int-3.1.0 hdfs router
 */
public interface StateStoreCache {

  /**
   * Load the cache from the State Store. Called by the cache update service
   * when the data has been reloaded.
   *
   * @param force If we force the load.
   * @return If the cache was loaded successfully.
   * @throws IOException If there was an error loading the cache.
   */
  boolean loadCache(boolean force) throws IOException;
}
