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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test consistency of reads while accessing an ObserverNode.
 * The tests are based on traditional (non fast path) edits tailing.
 */
public class TestConsistentReadsObserver {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestConsistentReadsObserver.class.getName());

  private static Configuration conf;
  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  private final Path testPath= new Path("/TestConsistentReadsObserver");

  @BeforeClass
  public static void startUpCluster() throws Exception {
    conf = new Configuration();
    // disable fast tailing here because this test's assertions are based on the
    // timing of explicitly called rollEditLogAndTail. Although this means this
    // test takes some time to run
    // TODO: revisit if there is a better way.
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 0, false);
    dfsCluster = qjmhaCluster.getDfsCluster();
  }

  @Before
  public void setUp() throws Exception {
    setObserverRead(true);
  }

  @After
  public void cleanUp() throws IOException {
    dfs.delete(testPath, true);
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testMsyncSimple() throws Exception {
    // 0 == not completed, 1 == succeeded, -1 == failed
    final AtomicInteger readStatus = new AtomicInteger(0);

    // Making an uncoordinated call, which initialize the proxy
    // to Observer node.
    dfs.getClient().getHAServiceState();
    dfs.mkdir(testPath, FsPermission.getDefault());
    assertSentTo(0);

    Thread reader = new Thread(
        new Runnable() {
          @Override
          public void run() {
            try {
              // this read will block until roll and tail edits happen.
              dfs.getFileStatus(testPath);
              readStatus.set(1);
            } catch (IOException e) {
              e.printStackTrace();
              readStatus.set(-1);
            }
          }
        });

    reader.start();
    // the reader is still blocking, not succeeded yet.
    assertEquals(0, readStatus.get());
    dfsCluster.rollEditLogAndTail(0);
    // wait a while for all the change to be done
    GenericTestUtils.waitFor(
        new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return readStatus.get() != 0;
          }
        }, 100, 10000);

    // the reader should have succeed.
    assertEquals(1, readStatus.get());
  }

  // @Ignore("Move to another test file")
  @Test
  public void testUncoordinatedCall() throws Exception {
    // make a write call so that client will be ahead of
    // observer for now.
    dfs.mkdir(testPath, FsPermission.getDefault());

    // a status flag, initialized to 0, after reader finished, this will be
    // updated to 1, -1 on error
    final AtomicInteger readStatus = new AtomicInteger(0);

    // create a separate thread to make a blocking read.
    Thread reader = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // this read call will block until server state catches up. But due to
          // configuration, this will take a very long time.
          dfs.getClient().getFileInfo("/");
          readStatus.set(1);
          fail("Should have been interrupted before getting here.");
        } catch (IOException e) {
          e.printStackTrace();
          readStatus.set(-1);
        }
      }
    });
    reader.start();

    long before = Time.now();
    dfs.getClient().datanodeReport(HdfsConstants.DatanodeReportType.ALL);
    long after = Time.now();

    // should succeed immediately, because datanodeReport is marked an
    // uncoordinated call, and will not be waiting for server to catch up.
    assertTrue(after - before < 200);
    // by this time, reader thread should still be blocking, so the status not
    // updated
    assertEquals(0, readStatus.get());
    Thread.sleep(5000);
    // reader thread status should still be unchanged after 5 sec...
    assertEquals(0, readStatus.get());
    // and the reader thread is not dead, so it must be still waiting
    assertEquals(Thread.State.WAITING, reader.getState());
    reader.interrupt();
  }

  private void assertSentTo(int nnIdx) throws IOException {
    assertTrue("Request was not sent to the expected namenode " + nnIdx,
        HATestUtil.isSentToAnyOfNameNodes(dfs, dfsCluster, nnIdx));
  }

  private static void setObserverRead(boolean flag) throws Exception {
    dfs = HATestUtil.configureObserverReadFs(
        dfsCluster, conf, ObserverReadProxyProvider.class, flag);
  }
}
