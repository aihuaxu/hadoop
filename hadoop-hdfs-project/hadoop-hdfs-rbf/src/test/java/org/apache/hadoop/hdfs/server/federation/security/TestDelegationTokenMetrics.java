package org.apache.hadoop.hdfs.server.federation.security;

import static org.apache.hadoop.hdfs.server.federation.metrics.TestFederationMetrics.FEDERATION_BEAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationMBean;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.Before;
import org.junit.Test;

public class TestDelegationTokenMetrics {
  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** Random Router for this federated cluster. */
  private static MiniRouterDFSCluster.RouterContext router;

  @Before
  public void setup() throws Exception {
    cluster = new MiniRouterDFSCluster(false, 1);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    routerConf.set(RBFConfigKeys.DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS,
        TestRouterSecurityManager
            .TestDelegationTokenSecretManager.class.getName());
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    router = cluster.getRandomRouter();
  }

  @Test
  public void testDelegationTokenMetricsJMX() throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createUserForTesting("router", new String[]{"router_group"}));
    DFSClient routerClient = router.getClient();
    ClientProtocol clientProtocol = routerClient.getNamenode();
    Token dt = clientProtocol.getDelegationToken(new Text("router"));
    FederationMBean bean = FederationTestUtils.getBean(
        FEDERATION_BEAN, FederationMBean.class);
    assertEquals(1, bean.getCurrentTokensCount());
    Object[] topOwner =
        (Object[]) JSON.parse(bean.getTopTokenRealOwners());
    assertEquals(1, topOwner.length);
    String pair = (String)topOwner[0];
    assertTrue(pair.contains("router"));
    assertTrue(pair.contains("1"));

    clientProtocol = routerClient.getNamenode();
    clientProtocol.renewDelegationToken(dt);
    assertEquals(1, bean.getCurrentTokensCount());

    clientProtocol.cancelDelegationToken(dt);
    assertEquals(0, bean.getCurrentTokensCount());
    topOwner =
        (Object[]) JSON.parse(bean.getTopTokenRealOwners());
    assertEquals(0, topOwner.length);
  }
}
