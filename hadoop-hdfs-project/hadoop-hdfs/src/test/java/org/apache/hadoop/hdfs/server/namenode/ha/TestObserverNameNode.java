/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestObserverNameNode {
  private Configuration conf;
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster dfsCluster;
  private NameNode[] namenodes;
  private Path testPath;
  private Path testPath2;
  private Path testPath3;

  /** These are set in each individual test case */
  private DistributedFileSystem dfs;
  private RetryInvocationHandler handler;
  private ObserverReadProxyProvider provider;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    // Disable observer read first, and enable it in some of the individual
    // tests
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED,
        false);

    setUpCluster(1);

    testPath = new Path("/test");
    testPath2 = new Path("/test2");
    testPath3 = new Path("/test3");
  }

  @After
  public void tearDown() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testSimpleRead() throws Exception {
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    try {
      dfs.getFileStatus(testPath);
      assertSentTo(0);
    } catch (FileNotFoundException e) {
      fail("Should not throw FileNotFoundException");
    }

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);
  }

  @Test
  public void testFNF() throws Exception {
    setObserverRead(true);

    dfs.createNewFile(testPath);
    try {
      dfs.open(testPath);
      assertSentTo(0);
    } catch (FileNotFoundException e) {
      fail("Should not throw FileNotFoundException");
    }

    try {
      dfs.getFileStatus(testPath2);
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Continue
    }
    assertSentTo(0);

    try {
      dfs.listStatus(testPath2);
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Continue
    }
    assertSentTo(0);

    try {
      dfs.listLocatedStatus(testPath2);
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException e) {
      // Continue
    }
    assertSentTo(0);
  }

  @Test
  public void testFailOver() throws Exception {
    setObserverRead(false);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive();

    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(1);
    dfs.getFileStatus(testPath);
    assertSentTo(1);
  }

  @Test
  public void testDoubleFailOver() throws Exception {
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
    dfs.mkdir(testPath2, FsPermission.getDefault());
    assertSentTo(0);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive(1);

    rollEditLogAndTail(1);
    dfs.getFileStatus(testPath2);
    assertSentTo(2);
    dfs.mkdir(testPath3, FsPermission.getDefault());
    assertSentTo(1);

    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath3);
    assertSentTo(2);
    dfs.delete(testPath3, false);
    assertSentTo(0);
  }

  @Test
  public void testObserverShutdown() throws Exception {
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    // Shutdown the observer - requests should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    // Start the observer again - requests should go to observer
    dfsCluster.restartNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
  }

  @Test
  public void testObserverFailOverAndShutdown() throws Exception {
    // Test the case when there is a failover before ONN shutdown
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    dfsCluster.waitActive();

    // Shutdown the observer - requests should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(1);

    // Start the observer again - requests should go to observer
    dfsCluster.restartNameNode(2);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
  }


  @Test
  public void testMultiObserver() throws Exception {
    setUpCluster(2);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentToOne(2, 3);

    dfs.mkdir(testPath2, FsPermission.getDefault());
    rollEditLogAndTail(0);

    // Shutdown first observer, request should go to the second one
    dfsCluster.shutdownNameNode(2);
    dfs.listStatus(testPath2);
    assertSentTo(3);

    // Restart the first observer
    dfsCluster.restartNameNode(2);
    dfs.listStatus(testPath);
    assertSentToOne(2, 3);

    dfs.mkdir(testPath3, FsPermission.getDefault());
    rollEditLogAndTail(0);

    // Now shutdown the second observer, request should go to the first one
    dfsCluster.shutdownNameNode(3);
    dfs.listStatus(testPath3);
    assertSentTo(2);

    // Shutdown both, request should go to active
    dfsCluster.shutdownNameNode(2);
    dfs.listStatus(testPath3);
    assertSentTo(0);
  }

  @Test
  public void testObserverStale() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_HA_OBSERVER_STALE_LIMIT_MS_KEY, 500);
    setUpCluster(1);
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.listStatus(testPath);
    assertSentTo(2);

    Thread.sleep(1000);
    dfs.listStatus(testPath);
    assertSentTo(0);
  }

  @Test
  public void testTransition() throws Exception {
    try {
      dfsCluster.transitionToActive(2);
      fail("Should get exception");
    } catch (ServiceFailedException e) {
      // pass
    }

    try {
      dfsCluster.transitionToStandby(2);
      fail("Should get exception");
    } catch (ServiceFailedException e) {
      // pass
    }
  }

  @Test
  public void testBootstrap() throws Exception {
    for (URI u : dfsCluster.getNameDirs(2)) {
      File dir = new File(u.getPath());
      assertTrue(FileUtil.fullyDelete(dir));
    }
    int rc = BootstrapStandby.run(
        new String[]{"-nonInteractive"},
        dfsCluster.getConfiguration(2)
    );
    assertEquals(0, rc);
  }

  private void setUpCluster(int numObservers) throws Exception {
    qjmhaCluster = new MiniQJMHACluster.Builder(conf)
        .setNumNameNodes(2)
        .setNumObservers(numObservers)
        .setNumDataNodes(3)
        .build();
    dfsCluster = qjmhaCluster.getDfsCluster();

    namenodes = new NameNode[2 + numObservers];
    for (int i = 0; i < namenodes.length; i++) {
      namenodes[i] = dfsCluster.getNameNode(i);
    }

    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);
  }

  private void assertSentTo(int nnIdx) {
    FailoverProxyProvider.ProxyInfo pi = provider.getLastProxy();
    assertEquals(pi.proxyInfo,
        getNameNode(nnIdx).getNameNodeAddress().toString());
  }

  private void assertSentToOne(int... nnIndices) {
    FailoverProxyProvider.ProxyInfo pi = provider.getLastProxy();
    int i = 0;
    for(int nnIdx : nnIndices) {
      if(pi.proxyInfo.equals(
          getNameNode(nnIdx).getNameNodeAddress().toString())) {
        i++;
      }
    }
    assertEquals(1, i);
  }

  private void setObserverRead(boolean flag) throws Exception {
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED,
        flag);
    dfs = HATestUtil.configureStaleReadFs(dfsCluster, conf, 0);
    handler = (RetryInvocationHandler) Proxy.getInvocationHandler(
        dfs.getClient().getNamenode());
    provider = (ObserverReadProxyProvider) handler.getProxyProvider();
  }

  private void rollEditLogAndTail(int indexForActiveNN) throws Exception {
    getNameNode(indexForActiveNN).getRpcServer().rollEditLog();
    for (int i = 2; i < namenodes.length; i++) {
      getNameNode(i).getNamesystem().getEditLogTailer().doTailEdits();
    }
  }

  private NameNode getNameNode(int idx) {
    return dfsCluster.getNameNode(idx);
  }

}
