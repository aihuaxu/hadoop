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

import com.uber.m3.tally.Scope;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote2;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.util.M3MetricsPublisher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class TestMetricsPublisher {
  private static final long reportInterval = 500;
  private MiniDFSCluster cluster;
  private final DataNodeFaultInjector injector = DataNodeFaultInjector.get();
  private final Random random = new Random();
  private final long delayTime = 3000;
  private DistributedFileSystem fs;

  private M3MetricsPublisher instance;
  private final Map<String, Long> counterMap = new ConcurrentHashMap<>();
  private final Map<String, Double> gaugeMap = new ConcurrentHashMap<>();

  @Before
  public void setUp() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_METRICS_ENABLED_KEY, true);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_METRICS_EMIT_READ_TIME_THRESHOLD_KEY, 1000);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_METRICS_EMIT_READ_PACKET_TIME_THRESHOLD_KEY, 1000);
    // use testing reporter
    updateMetricsPublisher(new DfsClientConf(conf));

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    delayDataNodeRead();
  }

  @After
  public void tearDown() throws Exception {
    resetDataNode();
    counterMap.clear();
    gaugeMap.clear();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void updateMetricsPublisher(DfsClientConf conf) throws Exception {
    instance = M3MetricsPublisher.getInstance(conf);
    final Scope dnParentScope = DFSTestUtil.createM3ClientForTest(
            reportInterval, counterMap, gaugeMap);
    final Scope scope = DFSTestUtil.createM3ClientForTest(
            reportInterval, counterMap, gaugeMap);

    Field dnScopeField = M3MetricsPublisher.class.getDeclaredField("dnParentScope");
    dnScopeField.setAccessible(true);
    dnScopeField.set(instance, dnParentScope);

    Field scopeField = M3MetricsPublisher.class.getDeclaredField("scope");
    scopeField.setAccessible(true);
    scopeField.set(instance, scope);
  }

  private void delayDataNodeRead() {
    DataNodeFaultInjector injector = Mockito.mock(DataNodeFaultInjector.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(delayTime);
        return null;
      }
    }).when(injector).delaySendBlock();
    DataNodeFaultInjector.set(injector);
  }

  private void resetDataNode() {
    DataNodeFaultInjector.set(injector);
  }

  private byte[] getFileContent() {
    byte[] content = new byte[1024];
    random.nextBytes(content);
    return content;
  }

  @Test
  public void testSlowReadMetrics() throws Exception {
    final Path file = new Path("/foo");
    final byte[] fileContent = getFileContent();
    DFSTestUtil.writeFile(fs, file, fileContent);

    // make sure we do not have any slow read captured in the metrics map
    Assert.assertTrue(counterMap.isEmpty());
    Assert.assertTrue(gaugeMap.isEmpty());

    // read the whole file, and check the metrics
    byte[] result = DFSTestUtil.readFileBuffer(fs, file);
    Assert.assertArrayEquals(fileContent, result);

    // make sure the metrics has been reported
    Thread.sleep(reportInterval * 2);
    // check the metrics
    String slowReadNumKey = DFSInputStream.NUM_SLOW_READ + ":{}";
    long slowReadNum = counterMap.get(slowReadNumKey);
    Assert.assertEquals(1L, slowReadNum);
    String slowReadTimeKey = DFSInputStream.SLOW_READ_TIME + ":{}";
    double slowReadTime = gaugeMap.get(slowReadTimeKey);
    Assert.assertTrue(slowReadTime > delayTime && slowReadTime < delayTime * 2);

    // read again
    DFSTestUtil.readFileBuffer(fs, file);
    Thread.sleep(reportInterval * 2);
    slowReadNum = counterMap.get(slowReadNumKey);
    // in StatsReporterForTest we simply add reported counter together based on
    // their keys, so the current slow read number should be 2
    Assert.assertEquals(2L, slowReadNum);
    slowReadTime = gaugeMap.get(slowReadTimeKey);
    // we should have 2 slow read each is greater than 3s. StatsReporterForTest
    // simply adds them together so the current slowReadTime should be > 6s
    Assert.assertTrue("slowReadTime:" + slowReadTime, slowReadTime > delayTime * 2);

    // do a position read
    byte[] preadResult = new byte[512];
    try (FSDataInputStream in = fs.open(file)) {
      in.read(512, preadResult, 0, 512);
    }
    Thread.sleep(reportInterval * 2);
    // pread will not affect stateful read metrics
    slowReadNum = counterMap.get(slowReadNumKey);
    Assert.assertEquals(2L, slowReadNum);
    // check metrics for pread
    String slowPreadNumKey = DFSInputStream.NUM_SLOW_PREAD + ":{}";
    Assert.assertEquals(1L, (long) counterMap.get(slowPreadNumKey));
    String slowPreadTimeKey = DFSInputStream.SLOW_PREAD_TIME + ":{}";
    double slowPreadTime = gaugeMap.get(slowPreadTimeKey);
    Assert.assertTrue("slowPreadTime:" + slowPreadTime,
            slowPreadTime > delayTime && slowPreadTime < delayTime * 2);

    // do another pread
    try (FSDataInputStream in = fs.open(file)) {
      in.read(128, preadResult, 0, 512);
    }
    // instead of wait for report interval, close publisher
    instance.closeForTest();
    Assert.assertEquals(2L, (long) counterMap.get(slowPreadNumKey));
    slowPreadTime = gaugeMap.get(slowPreadTimeKey);
    Assert.assertTrue("slowPreadTime:" + slowPreadTime,
            slowPreadTime > delayTime * 2);

    // check the block reader metrics
    String dn = cluster.getDataNodes().get(0).getDatanodeId().getHostName();
    String slowBlockReaderNumKey = BlockReaderRemote2.NUM_SLOW_PACKET + ":" +
            Collections.singletonMap("datanode", dn);
    String slowBlockReaderTimeKey = BlockReaderRemote2.SLOW_PACKET_TIME + ":" +
            Collections.singletonMap("datanode", dn);
    Assert.assertEquals(4L, (long) counterMap.get(slowBlockReaderNumKey));
    double slowReaderTime = gaugeMap.get(slowBlockReaderTimeKey);
    Assert.assertTrue("slow reader time: " + slowReaderTime,
            slowReaderTime > delayTime * 4);
  }
}
