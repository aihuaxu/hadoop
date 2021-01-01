package org.apache.hadoop.yarn.server.router.external.peloton;

import com.google.common.base.Supplier;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.MockRouter;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_CLIENTRM_ADDRESS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_RMADMIN_ADDRESS;
import static org.apache.hadoop.yarn.server.router.RouterConfigKeys.ROUTER_ADMIN_ADDRESS_KEY;
import static org.apache.hadoop.yarn.server.router.RouterConfigKeys.ROUTER_STORE_ENABLE;

public class TestRouterElectorBasedElectorService {

  private static final String ROUTER1_ADDRESS = "1.1.1.1:1";
  private static final String ROUTER1_NODE_ID = "test-router1";

  private static final String ROUTER2_ADDRESS = "0.0.0.0:0";
  private static final String ROUTER2_NODE_ID = "test-router2";

  Configuration conf ;
  TestingCluster zkCluster;

  MockRouter mockRouter1;

  MockRouter mockRouter2;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    conf = new Configuration();
    conf.setBoolean(ROUTER_STORE_ENABLE,false);
    conf.setBoolean(YarnConfiguration.ROUTER_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR, true);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
    conf.set(YarnConfiguration.ROUTER_HA_IDS, ROUTER1_NODE_ID + "," + ROUTER2_NODE_ID);

    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      conf.set(HAUtil.addSuffix(confKey, ROUTER1_NODE_ID), ROUTER1_ADDRESS);
      conf.set(HAUtil.addSuffix(confKey, ROUTER2_NODE_ID), ROUTER2_ADDRESS);
    }

    zkCluster = new TestingCluster(3);
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, zkCluster.getConnectString());
    zkCluster.start();
  }
  @After
  public void tearDown() throws Exception {
    if (mockRouter1 != null) {
      mockRouter1.stop();
    }
    if (mockRouter2 !=null) {
      mockRouter2.stop();
    }
  }

  @Test(timeout = 20000)
  public void testRouterStart() throws Exception {
    initRouter1();
    initRouter2();

    // wait for some time to make sure router2 will not become active;
    Thread.sleep(5000);
    waitFor(mockRouter1, HAServiceProtocol.HAServiceState.ACTIVE);
    waitNotFor(mockRouter2, HAServiceProtocol.HAServiceState.ACTIVE);
  }
  // 1. router1 active
  // 2. router2 not active

  // 3. stop router1
  // 4. router2 not active
  @Test(timeout = 20000)
  public void testRouterShutDownCauseFailover() throws Exception {
    initRouter1();
    initRouter2();
    // wait for some time to make sure router2 will not become active;
    Thread.sleep(5000);
    waitFor(mockRouter1, HAServiceProtocol.HAServiceState.ACTIVE);
    waitNotFor(mockRouter2, HAServiceProtocol.HAServiceState.ACTIVE);

    mockRouter1.stop();
    // router2 should become active;
    waitFor(mockRouter2, HAServiceProtocol.HAServiceState.ACTIVE);
    waitNotFor(mockRouter1, HAServiceProtocol.HAServiceState.ACTIVE);
  }

  // 1. router1 active
  // 2. restart zk cluster
  // 3. router1 will first relinquish leadership and re-acquire leadership
  @Test
  public void testZKClusterDown() throws Exception {
    initRouter1();
    // stop zk cluster
    zkCluster.stop();
    waitNotFor(mockRouter1, HAServiceProtocol.HAServiceState.ACTIVE);

    Collection<InstanceSpec> instanceSpecs = zkCluster.getInstances();
    zkCluster = new TestingCluster(instanceSpecs);
    zkCluster.start();
    // router becomes active again
    waitFor(mockRouter1, HAServiceProtocol.HAServiceState.ACTIVE);
  }

  private void initRouter1() throws Exception {
    conf.set(ROUTER_CLIENTRM_ADDRESS,"0.0.0.0:8088");
    conf.set(ROUTER_RMADMIN_ADDRESS, "0.0.0.0:8089");
    conf.set(ROUTER_ADMIN_ADDRESS_KEY, "0.0.0.0:8090");
    mockRouter1 = startRouter(ROUTER1_NODE_ID);
  }

  private void initRouter2() throws Exception {
    conf.set(ROUTER_CLIENTRM_ADDRESS,"0.0.0.0:8091");
    conf.set(ROUTER_RMADMIN_ADDRESS, "0.0.0.0:8092");
    conf.set(ROUTER_ADMIN_ADDRESS_KEY, "0.0.0.0:8093");
    mockRouter2 = startRouter(ROUTER2_NODE_ID);
  }

  private MockRouter startRouter(String routerId) throws Exception{
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    yarnConf.set(YarnConfiguration.ROUTER_HA_ID, routerId);
    MockRouter router = new MockRouter(yarnConf, true);
    router.enableTest();
    router.init(yarnConf);
    router.start();
    return router;
  }

  private void waitFor(final Router router,
      final HAServiceProtocol.HAServiceState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        try {
          return router.getRouterHAContext().getHAServiceState()
              .equals(state);
        } catch (Exception e) {
        }
        return false;
      }
    }, 2000, 15000);
  }

  private void waitNotFor(final Router router,
      final HAServiceProtocol.HAServiceState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override public Boolean get() {
        try {
          return !router.getRouterHAContext().getHAServiceState()
              .equals(state);
        } catch (Exception e) {
        }
        return false;
      }
    }, 2000, 15000);
  }
}
