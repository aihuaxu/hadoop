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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;

@Ignore
public class TestObserverNameNode {
  private Configuration conf;
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster dfsCluster;
  private NameNode[] namenodes;
  private Map<String, Integer> namenodeIndex;
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
  public void testConfigChange() throws Exception {
    setObserverRead(true);

    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.getFileStatus(testPath);
    assertSentTo(2);

    // Change the flag. Now request should go not to observer.
    setObserverRead(false);
    dfs.getFileStatus(testPath);
    assertSentTo(0);

    // Re-enable observer read, request should go to observer again.
    setObserverRead(true);
    dfs.getFileStatus(testPath);
    assertSentTo(2);
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
  public void testPermissionDenied() throws Exception {
    setObserverRead(true);

    // Test read retries on active namenode when permission denies
    dfs.createNewFile(testPath);
    dfs.setPermission(testPath, FsPermission.valueOf("r---------"));
    rollEditLogAndTail(0);

    // Simulate read by a regular user other than superuser
    UserGroupInformation FAKE_UGI_A =
            UserGroupInformation.createUserForTesting(
                    "myuser", new String[]{"group1"});

    String msg = FAKE_UGI_A.doAs(new PrivilegedAction<String>() {
      public String run() {
        try {
          DFSClient client = new DFSClient(DFSUtilClient.getNNAddress(conf), conf);
          client.open(testPath.toString());
          fail("Should throw AccessControlException");
        } catch (AccessControlException e) {
          // Expect to have this exception
          return e.getMessage();
        } catch (IOException e) {
          fail("Should not through IOException");
        }
        return null;
      }
    });

    assertTrue(msg.contains("Permission denied"));
    assertSentTo(0);

    // Test read normally from Observer
    try {
      dfs.getFileStatus(testPath);
    } catch (AccessControlException e) {
      fail("Should not throw AccessControlException");
    }

    assertSentTo(2);
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
  public void testObserverSafeMode() throws Exception {
    setUpCluster(1);
    setObserverRead(true);

    // Create a new file - the request should go to active.
    dfs.createNewFile(testPath);
    assertSentTo(0);

    rollEditLogAndTail(0);
    dfs.open(testPath);
    assertSentTo(2);

    // Set observer to safe mode.
    dfsCluster.getFileSystem(2).setSafeMode(SafeModeAction.SAFEMODE_ENTER);

    // Mock block manager for observer to generate some fake blocks which
    // will trigger the (retriable) safe mode exception.
    BlockManager bmSpy = NameNodeAdapter.spyOnBlockManager(namenodes[2]);
    doAnswer(new Answer<LocatedBlocks>() {
      @Override
      public LocatedBlocks answer(InvocationOnMock invocation) {
        ExtendedBlock b = new ExtendedBlock("fake-pool", new Block(12345L));
        LocatedBlock fakeBlock = new LocatedBlock(b, new DatanodeInfo[0]);
        List<LocatedBlock> fakeBlocks = new ArrayList<>();
        fakeBlocks.add(fakeBlock);
        return new LocatedBlocks(0, false, fakeBlocks, null, true, null);
      }
    }).when(bmSpy).createLocatedBlocks(Mockito.<BlockInfo[]> any(), anyLong(),
        anyBoolean(), anyLong(), anyLong(), anyBoolean(), anyBoolean(),
        Mockito.<FileEncryptionInfo> any());

    // Open the file again - it should throw retriable exception and then
    // failover to active.
    dfs.open(testPath);
    assertSentTo(0);

    // Remove safe mode on observer, request should still go to it.
    dfsCluster.getFileSystem(2).setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    dfs.open(testPath);
    assertSentTo(2);
  }

  @Test
  public void testBlockMissingRetry() throws Exception {
    setUpCluster(1);
    setObserverRead(true);

    FSDataOutputStream out = dfs.create(testPath);
    out.writeUTF("hello world");
    out.close();

    assertSentTo(0);

    rollEditLogAndTail(0);
    final FileStatus status = dfs.getFileStatus(testPath);
    assertSentTo(2);

    // Mock block manager for observer to generate some fake blocks which
    // will trigger the block missing exception.
    final LocatedBlocks blocks = namenodes[0].getRpcServer()
        .getBlockLocations(testPath.toString(), 0, status.getLen());
    BlockManager bmSpy = NameNodeAdapter.spyOnBlockManager(namenodes[2]);
    doAnswer(new Answer<LocatedBlocks>() {
      @Override
      public LocatedBlocks answer(InvocationOnMock invocation) {
        List<LocatedBlock> fakeBlocks = new ArrayList<>();
        // Remove the datanode info for the only block so it will throw
        // BlockMissingException and retry.
        LocatedBlock b = blocks.get(0);
        fakeBlocks.add(new LocatedBlock(b.getBlock(), new DatanodeInfo[0],
            null, null, b.getStartOffset(), false, null));
        return new LocatedBlocks(status.getLen(), false, fakeBlocks, null,
            true, null);
      }
    }).when(bmSpy).createLocatedBlocks(Mockito.<BlockInfo[]> any(), anyLong(),
        anyBoolean(), anyLong(), anyLong(), anyBoolean(), anyBoolean(),
        Mockito.<FileEncryptionInfo> any());

    FSDataInputStream in = dfs.open(testPath);
    assertEquals("hello world", in.readUTF());
    assertSentTo(0);
  }

  @Test
  public void testActiveRetriableException() throws Exception {
    // When observer is disabled, client should keep retrying the active NN.
    testRetriableExceptionHelper(false, 0, 0);
  }

  @Test
  public void testObserverRetriableException() throws Exception {
    // When observer is enabled, client should retry the next observer and
    // finally active.
    testRetriableExceptionHelper(true, 2, 3);
  }

  /**
   * Helper method for testing RetriableException when either observer is
   * enabled or disabled.
   *
   * @param observer whether to enable or disable observer read.
   * @param spyIdx the NN index to spy and throw RetriableException
   * @param expectedIdx the NN index that the RPC should succeed in the end.
   */
  private void testRetriableExceptionHelper(
      boolean observer, int spyIdx, int expectedIdx) throws Exception {
    setUpCluster(2);
    setObserverRead(observer);

    dfs.createNewFile(testPath);
    rollEditLogAndTail(0);

    final Cell cell = new Cell();
    BlockManager bmSpy = NameNodeAdapter.spyOnBlockManager(namenodes[spyIdx]);
    doAnswer(new Answer<LocatedBlocks>() {
      @Override
      public LocatedBlocks answer(InvocationOnMock invocation)
          throws Throwable {
        if (cell.count == 0) {
          cell.count++;
          throw new RetriableException("Should retry");
        }
        return new LocatedBlocks(0, false, new ArrayList<LocatedBlock>(),
            null, true, null);
      }
    }).when(bmSpy).createLocatedBlocks(Mockito.<BlockInfo[]> any(), anyLong(),
        anyBoolean(), anyLong(), anyLong(), anyBoolean(), anyBoolean(),
        Mockito.<FileEncryptionInfo> any());

    dfs.open(testPath);
    assertSentTo(expectedIdx);
  }

  private static class Cell {
    int count;
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
    namenodeIndex = new HashMap<>();
    for (int i = 0; i < namenodes.length; i++) {
      namenodes[i] = dfsCluster.getNameNode(i);
      namenodeIndex.put(namenodes[i].getNameNodeAddress().toString(), i);
    }

    dfsCluster.transitionToActive(0);
    dfsCluster.waitActive(0);
  }

  private void assertSentTo(int expectedIdx) {
    FailoverProxyProvider.ProxyInfo pi = provider.getLastProxy();
    int actualIdx = namenodeIndex.get(pi.proxyInfo);
    assertEquals(expectedIdx, actualIdx);
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
