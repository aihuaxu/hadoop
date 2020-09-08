package org.apache.hadoop.yarn.server.router.store.driver;

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
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils.getStateStoreConfiguration;

public class TestRouterStateStoreZK extends TestStateStoreDriverBase {
  private static TestingServer curatorTestingServer;
  private static CuratorFramework curatorFramework;

  @BeforeClass
  public static void setupCluster() throws Exception {
    initDriverConnection(curatorTestingServer, curatorFramework);
  }

  @AfterClass
  public static void tearDownCluster() {
    if (curatorFramework != null && curatorTestingServer != null) {
      curatorFramework.close();
      try {
        curatorTestingServer.stop();
      } catch (IOException e) {
      }
    }
  }

  @Before
  public void startup() throws IOException {
    removeAll(getStateStoreDriver());
  }

  @Test
  public void testInsert()
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(getStateStoreDriver());
  }
}
