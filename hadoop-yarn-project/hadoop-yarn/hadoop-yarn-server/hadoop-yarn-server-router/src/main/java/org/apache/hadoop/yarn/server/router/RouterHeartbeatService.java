package org.apache.hadoop.yarn.server.router;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.CachedRecordStore;
import org.apache.hadoop.store.PeriodicService;
import org.apache.hadoop.store.RecordStore;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.store.RouterRecordStore;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Service to periodically update the Router current state in the State Store.
 * Similar implementation as HDFS router 3.1.0
 */
public class RouterHeartbeatService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterHeartbeatService.class);

  /** Router we are hearbeating. */
  private final Router router;

  /**
   * Create a new Router heartbeat service.
   *
   * @param router Router to heartbeat.
   */
  public RouterHeartbeatService(Router router) {
    super(RouterHeartbeatService.class.getSimpleName());
    this.router = router;
  }

  /**
   * Trigger the update of the Router state asynchronously.
   */
  protected void updateStateAsync() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        updateStateStore();
      }
    }, "Router Heartbeat Async");
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Update the state of the Router in the State Store.
   */
  @VisibleForTesting
  synchronized void updateStateStore() {
    String routerId = router.getRouterId();
    if (routerId == null) {
      LOG.error("Cannot heartbeat for router: unknown router id");
      return;
    }
    if (isStoreAvailable()) {
      RouterRecordStore routerStore = router.getRouterRecordStore();
      try {
        RouterState record = RouterState.newInstance(
            routerId, router.getStartTime(), router.getRouterState());
        RouterHeartbeatRequest request =
            RouterHeartbeatRequest.newInstance(record);
        RouterHeartbeatResponse response = routerStore.routerHeartbeat(request);
        if (!response.getStatus()) {
          LOG.warn("Cannot heartbeat router {}", routerId);
        } else {
          LOG.debug("Router heartbeat for router {}", routerId);
        }
      } catch (IOException e) {
        LOG.error("Cannot heartbeat router {}: {}", routerId, e.getMessage());
      }
    } else {
      LOG.warn("Cannot heartbeat router {}: State Store unavailable", routerId);
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    long interval = conf.getTimeDuration(
        RouterConfigKeys.ROUTER_HEARTBEAT_STATE_INTERVAL_MS,
        RouterConfigKeys.ROUTER_HEARTBEAT_STATE_INTERVAL_MS_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.setIntervalMs(interval);

    super.serviceInit(conf);
  }

  @Override
  public void periodicInvoke() {
    updateStateStore();
  }

  private boolean isStoreAvailable() {
    if (router.getRouterRecordStore() == null) {
      return false;
    }
    if (router.getStateStoreService() == null) {
      return false;
    }
    return router.getStateStoreService().isDriverReady();
  }
}
