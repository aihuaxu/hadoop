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
import org.apache.hadoop.hdfs.MetricsPublisherTestUtil.MetricsTestSink;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.util.M3MetricsPublisher;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdfs.DFSSlowReadHandlingMetrics.READ_THREADPOOL_REJECTION_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDFSInputStreamWithFastSwitchRead {

  private static final String TEST_FILE_NAME = "/testfile";
  private static final int TEST_FILE_LEN = 64 * 1024 * 15;
  private static final int BLOCK_SIZE = 64 * 1000 * 4;
  private final byte[] fileContent = new byte[TEST_FILE_LEN];
  private M3MetricsPublisher instance;

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
    conf.setInt(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY, 1);
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

  /**
   * When all the nodes are relatively slow, read should not fail
   * It should just use the last node available.
   */
  @Test(timeout=300000)
  public void testAllNodesAreSlow() throws Exception {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 1500);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 1500);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 1500);
    try {
      writeFile(cluster);
      DFSInputStream fin = validateSimpleRead(cluster);
      // 4 blocks * 2 switches
      assertEquals(8, fin.switchCount);
      fin.close();
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

  @Test(timeout=300000)
  public void testInterruptCurrentRead() throws Exception {
    Configuration conf = generateConfig();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 5000);
    cluster.getDataNodes().get(2).setDelayDataNodeForTest(true, 5000);
    try {
      ExecutorService e = Executors.newSingleThreadExecutor();
      Future<Boolean> f = e.submit(doRead(cluster));
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

  private Callable<Boolean> readInternal(final DFSInputStream in,
          final int chunkSize, final byte[] content, final long blockSize) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() {
        byte[] buf = new byte[content.length];
        synchronized (in) {
          try {
            in.seek(0);
            int offset = 0;
            while (offset < content.length) {
              int ret = readAndSeek(in, buf, offset, chunkSize, blockSize);
              if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
              } else if (ret == 0) {
                DFSClient.LOG.info("read 0 bytes");
              }
              offset += ret;
              DFSClient.LOG.info("Current offset: " + offset);
            }
            Assert.assertArrayEquals(content, buf);
          } catch (IOException e) {
            fail("Do read failed: " + e);
          }
        }
        return true;
      }
    };
  }

  private int readAndSeek(DFSInputStream in, byte[] buf, int offset,
          int chunkSize, long blockSize) throws IOException {
    final long pos = in.getPos();
    boolean back = false;
    if (pos % blockSize > 0) {
      // seek backwards so that we will re-create block reader
      in.seek(pos - 1);
      offset--;
      back = true;
    }
    int ret = in.read(buf, offset, Math.min(chunkSize, buf.length - offset));
    return back ? ret - 1 : ret;
  }

  private MetricsTestSink setupMetricPublisher(Configuration conf) throws Exception {
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_METRICS_ENABLED_KEY, true);
    DfsClientConf dconf = new DfsClientConf(conf);
    instance = M3MetricsPublisher.getInstance(dconf);
    // use testing reporter
    return MetricsPublisherTestUtil.createTestMetricsPublisher(dconf, 1000 * 60);
  }

  private static class DataNodeDelayThread extends Thread {
    private volatile boolean running;
    private final Map<DataNode, Boolean> map = new HashMap<>();
    private final List<DataNode> nodes;

    DataNodeDelayThread(List<DataNode> nodes) {
      this.nodes = new ArrayList<>(nodes);
      for (DataNode dn : nodes) {
        map.put(dn, false);
      }
      running = true;
    }

    @Override
    public void run() {
      int index = 0;
      while (running) {
        DataNode dn1 = nodes.get(index % nodes.size());
        DataNode dn2 = nodes.get((index + 1) % nodes.size());
        index += 2;
        delayDataNode(dn1);
        delayDataNode(dn2);
        try {
          if (running) {
            Thread.sleep(500);
          }
        } catch (InterruptedException ignored) {
        }
      }
    }

    private void delayDataNode(DataNode dn) {
      if (!map.get(dn)) {
        dn.setDelayDataNodeForTest(true, 100);
        map.put(dn, true);
      } else {
        dn.setDelayDataNodeForTest(false, 0);
        map.put(dn, false);
      }
    }

    void stopDelay() {
      running = false;
    }
  }

  private void resetClientThreadPoolAndMetrics() throws Exception {
    Field threadPoolField = DFSClient.class.getDeclaredField("READ_THREAD_POOL");
    threadPoolField.setAccessible(true);
    threadPoolField.set(null, null);
    DFSClient.getSlowReadHandlingMetrics().readOpsInCurThread.set(0L);
  }

  private void testParallelRead(boolean shareInputStream, boolean expectReject)
          throws Exception {
    resetClientThreadPoolAndMetrics();
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.FastSwitchRead.ENABLED, true);
    conf.setLong(HdfsClientConfigKeys.FastSwitchRead.THRESHOLD_MILLIS_KEY, 100);
    final int numReadPoolThreads = 5;
    conf.setInt(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY, numReadPoolThreads);
    // set block size to 128KB
    final int blockSize = 64 * 1024 * 2;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    MetricsTestSink sink = setupMetricPublisher(conf);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
            .format(true).build();
    cluster.waitActive();
    final DistributedFileSystem fs = cluster.getFileSystem();
    final byte[] content = new byte[blockSize * 2];
    new Random().nextBytes(content);
    final String file = "/test-file";
    try {
      DFSTestUtil.writeFile(fs, new Path(file), content);
      DFSSlowReadHandlingMetrics metrics = DFSClient.getSlowReadHandlingMetrics();
      assertEquals(0, metrics.getReadOpsInCurThread());

      // delay DataNodes
      DataNodeDelayThread t = null;
      if (expectReject) {
        // delay the DN longer to drain thread pool
        cluster.getDataNodes().get(0).setDelayDataNodeForTest(true, 200);
        cluster.getDataNodes().get(1).setDelayDataNodeForTest(true, 200);
      } else {
        t = new DataNodeDelayThread(cluster.getDataNodes());
        t.start();
      }

      final int numReads = numReadPoolThreads * 10;
      ExecutorService executor = Executors.newFixedThreadPool(numReads);
      ArrayList<Future<Boolean>> futures = new ArrayList<>();

      final Callable<Boolean> callable;
      final DFSInputStream fin;
      if (shareInputStream) {
        fin = fs.getClient().open(file);
        callable = readInternal(fin, 512, content, blockSize);
      } else {
        fin = null;
        callable = new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            byte[] readContent = DFSTestUtil.readFileBuffer(fs, new Path(file));
            Assert.assertArrayEquals(content, readContent);
            return true;
          }
        };
      }

      for (int i = 0; i < numReads; i++) {
        futures.add(executor.submit(callable));
      }
      for (int i = 0; i < numReads; i++) {
        Assert.assertTrue(futures.get(i).get());
      }

      DFSClient.LOG.info("readOpsInCurThread: " + metrics.getReadOpsInCurThread());
      // make sure the metrics was reported to M3
      instance.closeForTest();
      long rejectionNum = sink.getCounterValue(READ_THREADPOOL_REJECTION_COUNT);
      DFSClient.LOG.info("rejection number reported to metrics publisher: " + rejectionNum);

      if (expectReject) {
        assertTrue(metrics.getReadOpsInCurThread() > 0);
        assertEquals(metrics.getReadOpsInCurThread(), rejectionNum);
      } else {
        assertEquals(0, metrics.getReadOpsInCurThread());
        assertEquals(0, rejectionNum);
      }

      if (fin != null) {
        fin.close();
      }
      executor.shutdown();
      if (t != null) {
        t.stopDelay();
        t.interrupt();
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMaxoutThreadPool() throws Exception {
    testParallelRead(false, true);
  }

  @Test
  public void testShareDFSInputStream() throws Exception {
    testParallelRead(true, false);
  }
}
