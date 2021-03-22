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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.*;


/**
 * Test the Router creating overloaded connections to NameNodes.
 */
public class TestRouterConnectionOverload {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterConnectionOverload.class);

  private StateStoreDFSCluster cluster;

  @After
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void setupCluster(boolean overloadControl) throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 1);
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .metrics()
        .admin()
        .rpc()
        .build();

    // Reduce the number of RPC clients threads to overload the Router easy
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_CLIENT_THREADS_SIZE, 4);
    routerConf.setInt(RBFConfigKeys.DFS_ROUTER_IPC_CONNECTION_SIZE, 2);
    // Overload control
    routerConf.setBoolean(
        RBFConfigKeys.DFS_ROUTER_CLIENT_REJECT_OVERLOAD, overloadControl);

    // No need for datanodes as we use renewLease() for testing
    cluster.setNumDatanodesPerNameservice(0);

    cluster.addRouterOverrides(routerConf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();
  }

  /**
   * Test the physical IPC connections on NN side are controlled by the
   * configuration dfs.federation.ipc.connection.size from routers.
   */
  @Test
  public void testControlIpcConnections() throws Exception {
    setupCluster(false);

    List<Integer> numConnections = new ArrayList<>();

    Thread metricThread = new Thread( () -> {
      NameNodeRpcServer nnRpcServer =
              (NameNodeRpcServer)(cluster.getCluster().getNameNode(0).getRpcServer());
      RpcMetrics nnRpcMetrics =
              nnRpcServer.getClientRpcServer().getRpcMetrics();

      while (true) {
        try {
          Thread.sleep(1000);
          numConnections.add(nnRpcMetrics.numOpenConnections());
        } catch (Exception e) {
          // ignore
        }
      }
    });

    metricThread.start();
    makeRouterCall(200);
    Thread.sleep(10000);
    metricThread.interrupt();

    for (int numConnection : numConnections) {
      assertTrue("More connections created:" + numConnection,numConnection <= 2);
    }
  }

  private void makeRouterCall(int numOps)
      throws Exception {
    RouterContext routerContext = cluster.getRouters().get(0);
    URI address = routerContext.getFileSystemURI();
    Configuration conf = new HdfsConfiguration();
    makeRouterCall(address, conf, numOps);
  }

  /**
   * Submit requests in parallel to routers.
   * @param address Destination address.
   * @param conf Configuration of the client.
   * @param numOps Number of operations to submit.
   * @throws Exception If it cannot perform the test.
   */
  private void makeRouterCall(final URI address,
                              final Configuration conf,
                              final int numOps)
          throws Exception {

    final AtomicInteger overloadException = new AtomicInteger();
    ExecutorService exec = Executors.newFixedThreadPool(numOps);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < numOps; i++) {
      Future<?> future = exec.submit(new Runnable() {
        @Override
        public void run() {
          DFSClient routerClient = null;
          try {
            routerClient = new DFSClient(address, conf);
            ClientProtocol routerProto = routerClient.getNamenode();
            routerProto.getFileInfo("/");
          } catch (RemoteException re) {
            IOException ioe = re.unwrapRemoteException();
            assertTrue("Wrong exception: " + ioe,
                ioe instanceof StandbyException);
            assertExceptionContains("is overloaded", ioe);
            overloadException.incrementAndGet();
          } catch (IOException e) {
            fail("Unexpected exception: " + e);
          } finally {
            if (routerClient != null) {
              try {
                routerClient.close();
              } catch (IOException e) {
                LOG.error("Cannot close the client");
              }
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
  }
}
