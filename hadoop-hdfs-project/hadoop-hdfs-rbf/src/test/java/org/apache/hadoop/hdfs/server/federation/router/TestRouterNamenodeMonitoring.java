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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_STARTUP_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Test;

/**
 * Test namenodes monitor behavior in the Router.
 */
public class TestRouterNamenodeMonitoring {

  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  private static MembershipNamenodeResolver resolver;

  private String ns0;
  private String ns1;
  private long initializedTime;
  private Configuration routerConf;

  public void setUpNormalRouterCluster() throws Exception {
    // Build and start a federated cluster with HA enabled
    cluster = new StateStoreDFSCluster(true, 2);
    // Enable heartbeat service and local heartbeat
    routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .enableLocalHeartbeat(true)
        .heartbeat()
        .build();

    // Specify local node (ns0.nn1) to monitor
    ns0 = cluster.getNameservices().get(0);
    NamenodeContext context = cluster.getNamenodes(ns0).get(1);
    routerConf.set(DFS_NAMESERVICE_ID, ns0);
    routerConf.set(DFS_HA_NAMENODE_ID_KEY, context.getNamenodeId());

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    ns1 = cluster.getNameservices().get(1);
    String monitorNNs = getMonitorNNs(ns1);
    // override with the namenodes: ns1.nn0,ns1.nn1
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, monitorNNs);

    startCluster(false);
  }

  public void setUpRouterObserverCluster(int numNs) throws Exception {
    // Build and start a federated cluster with HA enabled and 1 observer
    cluster = new StateStoreDFSCluster(true, numNs, 2, 1);
    // Enable heartbeat without local heartbeat
    routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .enableLocalHeartbeat(false)
        .heartbeat()
        .build();

    routerConf.set(DFS_ROUTER_STARTUP_KEY, StartupOption.OBSERVER.toString());
    StringBuilder allMonitorNNs = new StringBuilder();
    for (String ns : cluster.getNameservices()) {
      String monitorNNs = getMonitorNNs(ns);
      if (allMonitorNNs.length() > 0) {
        allMonitorNNs.append(',');
      }
      allMonitorNNs.append(monitorNNs);
      // override with the namenodes: ns0.nn0,ns0.nn1,ns0.nn2
      routerConf.set(DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX + "." + ns,
          cluster.getNamenodes(ns).get(2).getNamenodeId());
    }
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, allMonitorNNs.toString());

    startCluster(true);
  }

  // return 1 ns with observer and 1 ns without observer
  public void setUpMixedRouterObserverCluster() throws Exception {
    // Build and start a federated cluster with HA enabled and 1 observer
    cluster = new StateStoreDFSCluster(true, 2, 2, 2);
    List<String> nss = cluster.getNameservices();
    cluster.removeNamenode(nss.get(0), "onn0");
    cluster.removeNamenode(nss.get(0), "onn1");
    // Enable heartbeat without local heartbeat
    routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .enableLocalHeartbeat(false)
        .heartbeat()
        .build();

    routerConf.set(DFS_ROUTER_STARTUP_KEY, StartupOption.OBSERVER.toString());
    StringBuilder allMonitorNNs = new StringBuilder();
    for (String ns : cluster.getNameservices()) {
      String monitorNNs = getMonitorNNs(ns);
      if (allMonitorNNs.length() > 0) {
        allMonitorNNs.append(',');
      }
      allMonitorNNs.append(monitorNNs);
    }

    // For ns0, unset observers
    routerConf.set(DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX + "." + nss.get(0), "");
    // For ns1, set up the observers
    routerConf.set(DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX + "." + nss.get(1),
        "onn0,onn1");

    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, allMonitorNNs.toString());
    startCluster(true);
  }

  public String getMonitorNNs(String nsId) {
    StringBuilder sb = new StringBuilder();
    for (NamenodeContext ctx : cluster.getNamenodes(nsId)) {
      String suffix = ctx.getConfSuffix();
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(suffix);
    }
    return sb.toString();
  }

  public void startCluster(boolean isObserverRouter) throws Exception {
    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters(isObserverRouter);
    cluster.waitClusterUp();

    routerContext = cluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();
    initializedTime = Time.now();
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
  public void testNamenodeMonitoring() throws Exception {
    setUpNormalRouterCluster();
    // Set nn0 to active for all nameservices
    for (String ns : cluster.getNameservices()) {
      cluster.switchToActive(ns, "nn0");
      cluster.switchToStandby(ns, "nn1");
    }

    Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
        .getRouter().getNamenodeHearbeatServices();
    // manually trigger the heartbeat
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    resolver.loadCache(true);
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(ns0);
    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId(ns1);

    // The modified date won't be updated in ns0.nn0 since it isn't
    // monitored by the Router.
    assertEquals("nn0", namespaceInfo0.get(1).getNamenodeId());
    assertTrue(namespaceInfo0.get(1).getDateModified() < initializedTime);

    // other namnodes should be updated as expected
    assertEquals("nn1", namespaceInfo0.get(0).getNamenodeId());
    assertTrue(namespaceInfo0.get(0).getDateModified() > initializedTime);

    assertEquals("nn0", namespaceInfo1.get(0).getNamenodeId());
    assertTrue(namespaceInfo1.get(0).getDateModified() > initializedTime);

    assertEquals("nn1", namespaceInfo1.get(1).getNamenodeId());
    assertTrue(namespaceInfo1.get(1).getDateModified() > initializedTime);
  }

  @Test
  public void testObserverMonitoringSingleNS() throws Exception {
    int numNs = 1;
    setUpRouterObserverCluster(1);
    // Set nn0 to active for all nameservices
    String ns = cluster.getNameservices().get(0);
    cluster.switchToActive(ns, "nn0");
    cluster.switchToStandby(ns, "nn1");
    // nn2 is the observer node, whose HAState is Standby
    cluster.switchToStandby(ns, "onn0");

    Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
        .getRouter().getNamenodeHearbeatServices();
    assertEquals(numNs, heartbeatServices.size());
    // manually trigger the heartbeat
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    resolver.loadCache(true);
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(ns);
    // only observer node should be updated as expected
    assertEquals(1, namespaceInfo0.size());
    assertEquals("onn0", namespaceInfo0.get(0).getNamenodeId());
    assertTrue(namespaceInfo0.get(0).getDateModified() > initializedTime);
  }

  @Test
  public void testObserverMonitoringFederatedNS() throws Exception {
    int numNs = 2;
    setUpRouterObserverCluster(numNs);
    // Set nn0 to active for all nameservices
    for (String ns : cluster.getNameservices()) {
      cluster.switchToActive(ns, "nn0");
      cluster.switchToStandby(ns, "nn1");
      // nn2 is the observer node, whose HAState is Standby
      cluster.switchToStandby(ns, "onn0");
    }

    Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
        .getRouter().getNamenodeHearbeatServices();
    assertEquals(numNs, heartbeatServices.size());
    // manually trigger the heartbeat
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    resolver.loadCache(true);

    for (String ns : cluster.getNameservices()) {
      List<? extends FederationNamenodeContext> namespaceInfo0 =
          resolver.getNamenodesForNameserviceId(ns);

      // only observer node should be updated as expected
      assertEquals(1, namespaceInfo0.size());
      assertEquals("onn0", namespaceInfo0.get(0).getNamenodeId());
      assertTrue(namespaceInfo0.get(0).getDateModified() > initializedTime);
    }
  }

  // This test covers the case that some nameservice has no observers so
  // the router needs to monitor the active and standby namenodes
  @Test
  public void testObserverMonitoringInMixedFederatedNS() throws Exception {
    setUpMixedRouterObserverCluster();
    // Set nn0 to active for all nameservices
    List<String> nss = cluster.getNameservices();
    // set up ns0 without observer
    cluster.switchToActive(nss.get(0), "nn0");
    cluster.switchToStandby(nss.get(0), "nn1");
    // set up ns1 with 1 observer
    cluster.switchToActive(nss.get(1), "nn0");
    cluster.switchToStandby(nss.get(1), "nn1");
    // nn2 is the observer node, whose HAState is Standby
    cluster.switchToStandby(nss.get(1), "onn0");
    cluster.switchToStandby(nss.get(1), "onn1");

    Collection<NamenodeHeartbeatService> heartbeatServices = routerContext
        .getRouter().getNamenodeHearbeatServices();
    // will monitor 2 observer in ns1 + 2 namenodes in ns0
    assertEquals(4, heartbeatServices.size());
    // manually trigger the heartbeat
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    resolver.loadCache(true);

    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(nss.get(0));
    // active + standby namenodes should be updated as expected
    assertEquals(2, namespaceInfo0.size());
    assertEquals("nn0", namespaceInfo0.get(0).getNamenodeId());
    assertEquals("nn1", namespaceInfo0.get(1).getNamenodeId());
    assertTrue(namespaceInfo0.get(0).getDateModified() > initializedTime);
    assertTrue(namespaceInfo0.get(1).getDateModified() > initializedTime);

    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId(nss.get(1));
    // only observer node should be updated as expected
    assertEquals(2, namespaceInfo1.size());
    Set<String> namenodes =
        new HashSet<>(Arrays.asList(new String[]{"onn0", "onn1"}));
    assertTrue(namenodes.contains(namespaceInfo1.get(0).getNamenodeId()));
    assertTrue(namenodes.contains(namespaceInfo1.get(1).getNamenodeId()));
    assertTrue(!namespaceInfo1.get(0).getNamenodeId().equals(namespaceInfo1.get(1).getNamenodeId()));
    assertTrue(namespaceInfo1.get(0).getDateModified() > initializedTime);
    assertTrue(namespaceInfo1.get(1).getDateModified() > initializedTime);
  }
}
