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

import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.List;

/**
 * Test normal read when turning on fast switch
 */
public class TestParallelFastSwitchRead extends TestParallelReadUtil {
  @BeforeClass
  static public void setupCluster() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.FastSwitchRead.ENABLED, true);
    conf.setLong(HdfsClientConfigKeys.FastSwitchRead.THRESHOLD_MILLIS_KEY, 50);
    conf.setInt(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY, 50);
    ReadWorker.N_ITERATIONS = 128;

    final int numDataNodes = 3;
    setupCluster(numDataNodes, conf, numDataNodes);
    List<DataNode> datanodes = util.getCluster().getDataNodes();
    Assert.assertEquals(numDataNodes, datanodes.size());
    // delay one DataNode
    datanodes.get(0).setDelayDataNodeForTest(true, 100);
    datanodes.get(1).setDelayDataNodeForTest(true, 100);
  }

  @AfterClass
  static public void teardownCluster() throws Exception {
    DFSSlowReadHandlingMetrics metrics = util.getCluster().getFileSystem()
            .getClient().getSlowReadHandlingMetrics();
    Assert.assertEquals(0, metrics.getReadOpsInCurThread());
    TestParallelReadUtil.teardownCluster();
  }
}

