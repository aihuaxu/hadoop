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
package org.apache.hadoop.hdfs;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.net.unix.TemporarySocketDirectory;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDFSInputStreamWithFastSwitchRead {
  private DFSInputStream setupInputStream(MiniDFSCluster cluster) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    Path file = new Path("/testfile");
    int fileLength = 1000;
    byte[] fileContent = new byte[fileLength];
    for (int i = 0; i < fileLength; i++) {
      fileContent[i] = (byte) (i % 133);
    }
    FSDataOutputStream fout = fs.create(file);
    fout.write(fileContent);
    fout.close();

    return client.open("/testfile");
  }

  private Configuration generateConfig() {
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.FastSwitchRead.ENABLED, true);
    conf.setLong(HdfsClientConfigKeys.FastSwitchRead.THRESHOLD_MILLIS_KEY, 2000);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);
    return conf;
  }

  // Simple case -- Make sure we can read when 2 of the nodes are extremely slow.
  @Test(timeout=120000)
  public void testSimpleSwitch() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 100000000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 100000000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(false, 0);
    try {
      for (int i = 0; i < 5; i++) {
        DFSInputStream fin = setupInputStream(cluster);
        while (fin.available() > 0) {
          fin.read();
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * When all the nodes are relatively slow, read should not fail
   * It should just use the last node available.
   * @throws IOException
   */
  @Test(timeout=120000)
  public void testAllNodesAreSlow() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 5000);
    try {
      DFSInputStream fin = setupInputStream(cluster);
      while (fin.available() > 0) {
        fin.read();
      }
      // 2 blocks * 2 switches
      assertEquals(4, fin.switchCount);
      fin.close();
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=120000)
  public void testAbandonedConnectionsShouldBeCleanedUp() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 5000);
    try {
      final DFSInputStream fin = setupInputStream(cluster);
      while (fin.available() > 0) {
        fin.read();
      }
      fin.close();
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return fin.abandonedReaders.isEmpty();
        }
      }, 2000, 30000);
    } catch (InterruptedException | TimeoutException e) {
      fail("Readers are not closed in time.");
    } finally {
      cluster.shutdown();
    }
  }
}
