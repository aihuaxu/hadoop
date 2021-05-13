/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_STARTUP_KEY;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test retry behavior of the Router RPC Client.
 */
public class TestRouterForObserverRPCClientRetries {

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static MembershipNamenodeResolver resolver;
  private static ClientProtocol routerProtocol;

  @Before
  public void setUp() throws Exception {
    // Build and start a federated cluster with HA enabled and 1 observer
    cluster = new StateStoreDFSCluster(true, 1, 2, 1);
    // Enable heartbeat without local heartbeat
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .enableLocalHeartbeat(false)
        .heartbeat()
        .build();
    routerConf.set(DFS_ROUTER_STARTUP_KEY, StartupOption.OBSERVER.toString());

    StringBuilder allMonitorNNs = new StringBuilder();
    String ns = cluster.getNameservices().get(0);
    for (NamenodeContext ctx : cluster.getNamenodes(ns)) {
      String suffix = ctx.getConfSuffix();
      if (allMonitorNNs.length() != 0) {
        allMonitorNNs.append(",");
      }
      allMonitorNNs.append(suffix);
    }
    // override with the namenodes: ns0.nn0,ns0.nn1,ns0.nn2
    routerConf.set(DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX + "." + ns,
        cluster.getNamenodes(ns).get(2).getNamenodeId());
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, allMonitorNNs.toString());

    // override some settings for the client
    routerConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 2);
    routerConf.setInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 100);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters(true);
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();
    routerProtocol = routerContext.getClient().getNamenode();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testFailoverAndRetryWhenNameServiceDown() throws Exception {
    String ns = cluster.getNameservices().get(0);
    cluster.switchToActive(ns, "nn0");
    cluster.switchToStandby(ns, "nn1");
    // nn2 is the observer node, whose HAState is Standby
    cluster.switchToStandby(ns, "onn0");

    // shutdown the dfs cluster, a ConnectException will be thrown
    // and be understood as a FAILOVER_AND_RETRY action, thus no RETRY
    MiniDFSCluster dfsCluster = cluster.getCluster();
    dfsCluster.shutdownNameNodes();

    // Create a directory via the router
    String dirPath = "/testRetryWhenClusterisDown";
    try {
      routerProtocol.getFileInfo(dirPath);
      fail("Should have thrown RemoteException error.");
    } catch (RemoteException e) {
      assertFalse(
          e.getMessage().contains("No namenode available under nameservice"));
    }

    // Verify the retry times, it should not retry since a failover_and_retry
    // happened.
    FederationRPCMetrics rpcMetrics = routerContext.getRouter()
        .getRpcServer().getRPCMetrics();
    assertEquals(0, rpcMetrics.getProxyOpRetries());
  }

  @Test
  public void testShuffleObservers() throws Exception {
    FederationNamenodeContext nn1 = mock(FederationNamenodeContext.class);
    FederationNamenodeContext nn2 = mock(FederationNamenodeContext.class);
    when(nn1.getState()).thenReturn(FederationNamenodeServiceState.STANDBY);
    when(nn2.getState()).thenReturn(FederationNamenodeServiceState.STANDBY);

    List<? extends FederationNamenodeContext> list =
            Collections.unmodifiableList(Lists.newArrayList(nn1, nn2));

    Configuration conf = new Configuration();
    RouterRpcClient client = new RouterRpcClient(conf, null, null, null);
    List<FederationNamenodeContext> result = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      result.add(client.shuffleNameNodes(list).get(0));
    }
    assertTrue(result.contains(nn1));
    assertFalse(result.contains(nn2));

    conf.set(DFS_ROUTER_STARTUP_KEY, StartupOption.OBSERVER.toString());
    client = new RouterRpcClient(conf, null, null, null);
    result = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      result.add(client.shuffleNameNodes(list).get(0));
    }
    assertTrue(result.contains(nn1));
    assertTrue(result.contains(nn2));
  }
}
