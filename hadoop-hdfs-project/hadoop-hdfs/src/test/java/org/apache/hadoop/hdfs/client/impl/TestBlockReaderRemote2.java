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
package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.util.MetricsPublisher;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Random;
import static org.mockito.Mockito.*;

public class TestBlockReaderRemote2 extends TestBlockReaderBase {
  HdfsConfiguration createConf() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_METRICS_EMIT_READ_PACKET_TIME_THRESHOLD_KEY, -1);
    return conf;
  }

  @Test(timeout=60000)
  public void testMetrics() throws IOException {
    BlockReaderRemote2 remote2 = (BlockReaderRemote2)reader;
    MetricsPublisher mockPublisher = mock(MetricsPublisher.class);
    remote2.metricsPublisher = mockPublisher;

    Random random = new Random();
    byte [] buf = new byte[1024];
    int bytesRead = 0;
    while (bytesRead < blockData.length) {
      bytesRead += reader.read(buf, 0,
          Math.min(buf.length, random.nextInt(100)));
    }
    reader.close();
    verify(mockPublisher, atLeast(1)).emit(
        eq(MetricsPublisher.MetricType.GAUGE),
        Mockito.<String>any(),
        eq(BlockReaderRemote2.SLOW_PACKET_TIME),
        Mockito.anyLong());
    verify(mockPublisher, atLeast(1)).emit(
        eq(MetricsPublisher.MetricType.COUNTER),
        Mockito.<String>any(),
        eq(BlockReaderRemote2.NUM_SLOW_PACKET),
        eq(1L));
  }
}
