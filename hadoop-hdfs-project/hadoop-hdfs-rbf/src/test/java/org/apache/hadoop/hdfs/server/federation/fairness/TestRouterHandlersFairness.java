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
package org.apache.hadoop.hdfs.server.federation.fairness;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_ACQUIRE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_POLICY_CONTROLLER_DRIVER_CLASS;
import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the Router handlers fairness control rejects
 * requests when the handlers are overloaded. This feature is managed by
 * {@link RBFConfigKeys#DFS_ROUTER_NAMESERVICE_FAIRNESS_ENABLE}.
 */
public class TestRouterHandlersFairness {

  private static final Logger LOG =
          LoggerFactory.getLogger(TestRouterHandlersFairness.class);

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean fairnessEnable, boolean ha, Map<String, String> additionalProps)
          throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(ha, 2);
    Configuration routerConf = new RouterConfigBuilder()
            .stateStore()
            .rpc()
            .build();

    // Fairness control
    routerConf.setBoolean(
            RBFConfigKeys.DFS_ROUTER_NAMESERVICE_FAIRNESS_ENABLE, fairnessEnable);
    if (additionalProps != null) {
      for ( String propKey : additionalProps.keySet()) {
        routerConf.set(propKey, additionalProps.get(propKey));
      }
    }

    // With two name services configured, each nameservice has 1 permit and
    // fan-out calls have 1 permit.
    routerConf.setInt(
            RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY, 3);

    // Datanodes not needed for this test.
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  @Test
  public void testFairnessControlOff() throws Exception {
    setupCluster(false, false, null);
    startLoadTest(false);
  }

  @Test
  public void testFairnessControlOn() throws Exception {
    setupCluster(true, false, null);
    startLoadTest(true);
  }

  @Test
  public void testFairnessControlOnWithLimitMaxPolicy() throws Exception {
    Map<String, String> additionalProps = new HashMap<>();
    additionalProps.put(DFS_ROUTER_POLICY_CONTROLLER_DRIVER_CLASS,
            "org.apache.hadoop.hdfs.server.federation.fairness.LimitMaxPolicyController");
    // This is needed as the default timeout is 100 milliseconds, and likely not hit permit grant failure.
    additionalProps.put(DFS_ROUTER_FAIR_HANDLER_ACQUIRE_TIMEOUT_KEY, "0");
    setupCluster(true, false, additionalProps);
    startLoadTest(true);
  }

  private void startLoadTest(boolean fairness)
          throws Exception {

    // Concurrent requests
    startLoadTest(true, fairness);

    // Sequential requests
    startLoadTest(false, fairness);
  }

  private void startLoadTest(final boolean isConcurrent, final boolean fairness)
          throws Exception {

    RouterContext routerContext = cluster.getRandomRouter();
    URI address = routerContext.getFileSystemURI();
    Configuration conf = new HdfsConfiguration();
    int numOps = 100;

    final AtomicInteger overloadException = new AtomicInteger();
    ExecutorService exec = Executors.newFixedThreadPool(numOps);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < numOps; i++) {
      Future<?> future = exec.submit(() -> {
        DFSClient routerClient = null;
        try {
          // To allow runs to execute parallelly
          Thread.sleep(50);
          routerClient = new DFSClient(address, conf);
          String clientName = routerClient.getClientName();
          ClientProtocol routerProto = routerClient.getNamenode();
          if (isConcurrent) {
            invokeConcurrent(routerProto, clientName);
          } else {
            invokeSequential(routerProto);
          }
        } catch (RemoteException re) {
          IOException ioe = re.unwrapRemoteException();
          assertTrue("Wrong exception: " + ioe,
                  ioe instanceof StandbyException);
          assertExceptionContains("is overloaded for NS", ioe);
          overloadException.incrementAndGet();
        } catch (IOException e) {
          LOG.error("IOException : " + e);
          fail("Unexpected exception: " + e);
        } catch (InterruptedException e) {
          fail("InterruptedException: " + e);
        } finally {
          if (routerClient != null) {
            try {
              routerClient.close();
            } catch (IOException e) {
              LOG.error("Cannot close the client");
            }
          }
        }
      });
      futures.add(future);
    }
    // Wait until all the requests are done
    while (!futures.isEmpty()) {
      futures.remove(0).get();
    }
    exec.shutdown();

    if (fairness) {
      assertTrue(overloadException.get() > 0);
    } else {
      assertEquals(0, overloadException.get());
    }
  }

  private void invokeSequential(ClientProtocol routerProto) throws IOException {
    routerProto.getFileInfo("/test.txt");
  }

  private void invokeConcurrent(ClientProtocol routerProto, String clientName)
          throws IOException {
    routerProto.renewLease(clientName);
  }

}
