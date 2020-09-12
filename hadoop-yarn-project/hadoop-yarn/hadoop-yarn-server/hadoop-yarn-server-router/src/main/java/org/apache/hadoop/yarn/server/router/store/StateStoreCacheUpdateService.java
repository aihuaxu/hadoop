package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.PeriodicService;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Service to periodically update the {@link RouterStateStoreService}
 * cached information in the
 * {@link org.apache.hadoop.yarn.server.router.Router Router}.
 * This is for performance and removes the State Store from the critical path
 * in common operations.
 *
 * Similar implementation as HDFS router 3.1.0
 */
public class StateStoreCacheUpdateService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreCacheUpdateService.class);

  /** The service that manages the State Store connection. */
  private final RouterStateStoreService stateStore;


  /**
   * Create a new Cache update service.
   *
   * @param stateStore Implementation of the state store
   */
  public StateStoreCacheUpdateService(RouterStateStoreService stateStore) {
    super(StateStoreCacheUpdateService.class.getSimpleName());
    this.stateStore = stateStore;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.setIntervalMs(conf.getTimeDuration(
        RouterConfigKeys.ROUTER_CACHE_TIME_TO_LIVE_MS,
        RouterConfigKeys.ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT,
        TimeUnit.MILLISECONDS));

    super.serviceInit(conf);
  }

  @Override
  public void periodicInvoke() {
    LOG.debug("Updating State Store cache");
    stateStore.refreshCaches();
  }
}
