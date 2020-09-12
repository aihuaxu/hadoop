package org.apache.hadoop.yarn.server.router;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.yarn.server.router.store.RouterRecordStore;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils;
import org.apache.hadoop.yarn.server.router.store.driver.impl.StateStoreZooKeeperImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRouterHeartbeatService {
  private Router router;
  private final String routerId = "router1";
  private TestingServer testingServer;
  private CuratorFramework curatorFramework;

  @Before
  public void setup() throws Exception {
    router = new Router();
    router.setRouterId(routerId);
    Configuration conf = new Configuration();
    conf.setInt(RouterConfigKeys.ROUTER_CACHE_TIME_TO_LIVE_MS, 1);
    Configuration routerConfig =
        new RouterConfigBuilder(conf).stateStore().build();
    routerConfig.setClass(RouterConfigKeys.ROUTER_STORE_DRIVER_CLASS,
        StateStoreZooKeeperImpl.class, StateStoreDriver.class);

    testingServer = new TestingServer();
    String connectStr = testingServer.getConnectString();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(connectStr)
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();
    routerConfig.set(CommonConfigurationKeys.ZK_ADDRESS, connectStr);
    router.init(routerConfig);
    router.start();

    StateStoreTestUtils.waitStateStore(router.getStateStoreService(), TimeUnit.SECONDS.toMicros(10));
  }

  @Test
  public void testStateStoreUnavailable() throws IOException {
    curatorFramework.close();
    testingServer.stop();
    router.getStateStoreService().stop();
    // The driver is not ready
    assertFalse(router.getStateStoreService().isDriverReady());

    // Do a heartbeat, and no exception thrown out
    RouterHeartbeatService heartbeatService =
        new RouterHeartbeatService(router);
    heartbeatService.updateStateStore();
  }

  @Test
  public void testStateStoreAvailable() throws Exception {
    // The driver is ready
    RouterStateStoreService stateStore = router.getStateStoreService();
    assertTrue(router.getStateStoreService().isDriverReady());
    RouterRecordStore routerStore = router.getRouterRecordStore();

    // Do a heartbeat
    RouterHeartbeatService heartbeatService =
        new RouterHeartbeatService(router);
    heartbeatService.updateStateStore();

    // We should have a record
    stateStore.refreshCaches(true);
  }

  @After
  public void tearDown() throws IOException {
    if (curatorFramework != null) {
      curatorFramework.close();
    }
    if (testingServer != null) {
      testingServer.stop();
    }
    if (router != null) {
      router.shutDown();
    }
  }
}
