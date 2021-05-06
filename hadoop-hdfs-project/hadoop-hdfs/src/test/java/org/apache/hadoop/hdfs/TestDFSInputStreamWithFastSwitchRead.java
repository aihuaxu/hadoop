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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class TestDFSInputStreamWithFastSwitchRead {

  private static final String TEST_FILE_NAME = "/testfile";
  private static final int TEST_FILE_LEN = 64 * 1024 * 15;
  private static final int BLOCK_SIZE = 64 * 1000 * 4;
  private byte[] fileContent = new byte[TEST_FILE_LEN];

  private void writeFile(MiniDFSCluster cluster) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    Path file = new Path(TEST_FILE_NAME);
    Random rd = new Random();
    rd.nextBytes(fileContent);
    FSDataOutputStream fout = fs.create(file);
    fout.write(fileContent);
    fout.close();
  }

  private DFSInputStream validateSimpleRead(MiniDFSCluster cluster) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    DFSInputStream fin = client.open(TEST_FILE_NAME);
    return validateSimpleRead(fin);
  }

  private DFSInputStream validateSimpleRead(DFSInputStream fin) throws IOException {
    for (int i = 0; i < TEST_FILE_LEN; i++) {
      assertEquals(fileContent[i], (byte) fin.read());
    }
    return fin;
  }

  private DFSInputStream validateByteArrayRead(MiniDFSCluster cluster,
      int byteArraySize, int offset, int len) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    DFSInputStream fin = client.open(TEST_FILE_NAME);
    byte[] bytes = new byte[byteArraySize];
    int retlen = fin.read(bytes, offset, len);
    int fileContentIndex = 0;

    while(retlen > 0) {
      for (int i = 0; i < bytes.length; i++) {
        if (i < offset + retlen && i >= offset) {
          assertEquals(fileContent[fileContentIndex], bytes[i]);
          fileContentIndex++;
        } else if (i > offset + Math.min(len, 64 * 1024) || i < offset) {
          assertEquals(0, bytes[i]);
        }
      }
      retlen = fin.read(bytes, offset, len);
    }

    assertEquals(TEST_FILE_LEN, fileContentIndex);

    assertEquals(0, fin.available());
    return fin;
  }

  private DFSInputStream validateByteBufferRead(MiniDFSCluster cluster,
      int byteBufferSize) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.dfs;
    DFSInputStream fin = client.open(TEST_FILE_NAME);
    ByteBuffer bb = ByteBuffer.allocate(byteBufferSize);
    int fileContentIndex = 0;

    while(fin.read(bb) > 0) {
      bb.flip();
      while (bb.remaining() > 0) {
        assertEquals(fileContent[fileContentIndex], bb.get());
        fileContentIndex++;
      }
      bb.clear();
    }

    assertEquals(TEST_FILE_LEN, fileContentIndex);

    assertEquals(0, fin.available());
    return fin;
  }

  // Perform a byte to byte validation to make sure read is correct.
  private void testAllCases(MiniDFSCluster cluster) throws IOException {
    validateSimpleRead(cluster).close();

    // test byte array red
    int[][] params = new int[][]{
        {20, 0, 20},
        // With offset
        {50, 10, 30},
        // Buffer size > packet size
        {64 * 1024 * 2 + 1, 0, 64 * 1024 * 2 + 1},
    };
    for (int[] p : params) {
      validateByteArrayRead(cluster, p[0], p[1], p[2]).close();
    }

    // test ByteBuffer red
    for (int[] p : params) {
      validateByteBufferRead(cluster, p[0]).close();
    }
  }

  private Configuration generateConfig() {
    Configuration conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.FastSwitchRead.ENABLED, true);
    conf.setLong(HdfsClientConfigKeys.FastSwitchRead.THRESHOLD_MILLIS_KEY, 1000);
    // Make sure we have more than one block in the file.
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    return conf;
  }

  @Test(timeout=180000)
  public void testRead() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      writeFile(cluster);
      testAllCases(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  private Callable<Boolean> doReadFile(final DFSInputStream fin, final CountDownLatch counter) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          validateSimpleRead(fin);
          counter.countDown();
        } catch (IOException e) {
          fail("Do read failed: " + e);
        }
        return true;
      }
    };
  }

  @Test(timeout=180000)
  public void testConcurrentRead() throws Exception {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      writeFile(cluster);
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSClient client = fs.dfs;
      ExecutorService service = Executors.newFixedThreadPool(2);
      final CountDownLatch counter = new CountDownLatch(2);
      service.submit(doReadFile(client.open(TEST_FILE_NAME), counter));
      service.submit(doReadFile(client.open(TEST_FILE_NAME), counter));
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return counter.getCount() == 0;
        }
      }, 2000, 30000);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=180000)
  public void testConcurrentReadWithSmallMaxThreadPoolSize() throws Exception {
    Configuration conf = generateConfig();
    conf.setInt(HdfsClientConfigKeys.FastSwitchRead.THREADPOOL_MAX_SIZE_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      writeFile(cluster);
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSClient client = fs.dfs;
      ExecutorService service = Executors.newFixedThreadPool(2);
      final CountDownLatch counter = new CountDownLatch(2);
      service.submit(doReadFile(client.open(TEST_FILE_NAME), counter));
      service.submit(doReadFile(client.open(TEST_FILE_NAME), counter));
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override public Boolean get() {
          return counter.getCount() == 0;
        }
      }, 2000, 30000);
    } finally {
      cluster.shutdown();
    }
  }

  // Simple case -- Make sure we can read when 2 of the nodes are extremely slow.
  @Test(timeout=180000)
  public void testSwitch() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 100000000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 100000000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(false, 0);
    try {
      writeFile(cluster);
      testAllCases(cluster);
    } finally {
      cluster.shutdown();
    }
  }

  private Callable<Boolean> doRead(final MiniDFSCluster cluster) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          writeFile(cluster);
          DFSInputStream fin = validateSimpleRead(cluster);
          fin.close();
        } catch (IOException e) {
          fail("Do read failed: " + e);
        }
        return true;
      }
    };
  }

  /**
   * When all the nodes are relatively slow, read should not fail
   * It should just use the last node available.
   * @throws IOException
   */
  @Test(timeout=300000)
  public void testAllNodesAreSlow() throws Exception {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 5000);
    try {
//      writeFile(cluster);
//      DFSInputStream fin = validateSimpleRead(cluster);
//      // Times of switching should be related to the number of blocks.
//      // Round-up
//      int shouldSwitchTimes = ((TEST_FILE_LEN + BLOCK_SIZE - 1) / BLOCK_SIZE) * 2;
//      assertEquals(shouldSwitchTimes, fin.switchCount);
//      fin.close();
      ExecutorService e = Executors.newSingleThreadExecutor();
      Future f = e.submit(doRead(cluster));
      Thread.sleep(1000);
      f.cancel(true);
      Thread.sleep(10000);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=180000)
  public void testAbandonedConnectionsShouldBeCleanedUp() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 5000);
    try {
      writeFile(cluster);
      final DFSInputStream fin = validateSimpleRead(cluster);
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

  @Test(timeout=120000)
  public void testHandlingIOE() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      writeFile(cluster);
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSClient client = fs.dfs;
      DFSInputStream fin = client.open(TEST_FILE_NAME);

      BlockReader mockReader = mock(BlockReader.class);
      when(mockReader.read(any(ByteBuffer.class))).thenThrow(new IOException());


      for (int i = 0; i < TEST_FILE_LEN; i++) {
          if (i % 500 == 0) {
            // Inject an IOE once a while
            fin.setBlockReader(mockReader);
          }
        // Should recover from exception and create new blockreader.
        assertEquals(fileContent[i], (byte) fin.read());
      }

      assertEquals(0, fin.available());
      fin.close();
    } finally {
      cluster.shutdown();
    }
  }

  // Old executions in the thread pool should be ignored, reader should always get correct future.
  @Test(timeout=120000)
  public void testShouldReadCorrectFuture() throws IOException {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      writeFile(cluster);
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSClient client = fs.dfs;
      DFSInputStream fin = client.open(TEST_FILE_NAME);

      for (int i = 0; i < TEST_FILE_LEN; i++) {
        if (i % 500 == 0) {
          // Inject a bad future once a while
          Callable<DFSInputStream.ReadResult> readAction =
              fakeRead();
          fin.executorCompletionService.submit(readAction);
        }
        // Should ignore the bad future and provide valid read result.
        assertEquals(fileContent[i], (byte) fin.read());
      }

      assertEquals(0, fin.available());
      fin.close();
    } finally {
      cluster.shutdown();
    }
  }

  private Callable<DFSInputStream.ReadResult> fakeRead() {
    return new Callable<DFSInputStream.ReadResult>() {
      @Override
      public DFSInputStream.ReadResult call() {
        return new DFSInputStream.ReadResult(10, ByteBuffer.allocate(100), null, null);
      }
    };
  }
}

