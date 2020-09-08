package org.apache.hadoop.yarn.server.router.store;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.yarn.server.router.store.driver.impl.StateStoreZooKeeperImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils.waitStateStore;
import static org.apache.hadoop.yarn.server.router.store.driver.TestStateStoreDriverBase.fetchStateStore;
import static org.apache.hadoop.yarn.server.router.store.driver.TestStateStoreDriverBase.initDriverConnection;
import static org.junit.Assert.assertNotNull;


/**
 * Test the {@link RouterStateStoreService}
 * functionality.
 */
public class TestRouterStateStoreService {
  private static RouterStateStoreService stateStoreService;
  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;


  @BeforeClass
  public static void createBase() throws Exception {
    initDriverConnection(curatorTestingServer, curatorFramework);
    stateStoreService = fetchStateStore();
  }

  @AfterClass
  public static void destroyBase() throws Exception {
    if (stateStoreService != null) {
      stateStoreService.stop();
      stateStoreService.close();
      stateStoreService = null;
    }
    if (curatorFramework != null && curatorTestingServer != null ) {
      curatorFramework.close();
      try {
        curatorTestingServer.stop();
      } catch (IOException e) {
      }
    }
  }

  @Before
  public void setupBase() throws IOException, InterruptedException,
      InstantiationException, IllegalAccessException {
    assertNotNull(stateStoreService);
    // Wait for state store to connect
    stateStoreService.loadDriver();
    waitStateStore(stateStoreService, TimeUnit.SECONDS.toMillis(10));
  }
}
