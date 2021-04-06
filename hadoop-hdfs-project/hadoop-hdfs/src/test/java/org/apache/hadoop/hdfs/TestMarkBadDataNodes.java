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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BadDataNodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class TestMarkBadDataNodes {
  private final Configuration conf = new Configuration();
  protected static MiniDFSCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMarkBadDataNodes() throws Exception {
    // make sure all the datanodes are normal
    DatanodeManager datanodeManager = cluster.getNamesystem()
            .getBlockManager().getDatanodeManager();
    Set<DatanodeDescriptor> datanodes = datanodeManager.getDatanodes();
    DatanodeInfo toMark = null;
    for (DatanodeInfo node : datanodes) {
      Assert.assertFalse(node.isReportedBad());
      if (toMark == null) {
        toMark = node;
      }
    }

    // mark one DataNode as the bad one
    Assert.assertNotNull(toMark);
    BadDataNodeInfo badNode = new BadDataNodeInfo(toMark.getIpAddr(),
            toMark.getXferPort(), true);
    cluster.getFileSystem().markBadDataNodes(new BadDataNodeInfo[]{badNode});
    DatanodeInfo marked = datanodeManager.getDatanodeByXferAddr(
            toMark.getIpAddr(), toMark.getXferPort());
    Assert.assertTrue(marked.isReportedBad());

    // write a file and get its block locations.
    // make sure the bad one is in the end
    final Path file = new Path("/foo");
    DFSTestUtil.writeFile(cluster.getFileSystem(), file, "hello");
    LocatedBlocks locatedBlocks = cluster.getFileSystem().getClient()
            .getBlockLocations(file.toString(), 1);
    Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    DatanodeInfo[] locations = locatedBlocks.get(0).getLocations();
    Assert.assertEquals(3, locations.length);
    Assert.assertEquals(badNode.getXferPort(), locations[2].getXferPort());
    Assert.assertNotEquals(badNode.getXferPort(), locations[1].getXferPort());

    // reset to normal
    BadDataNodeInfo normalNode = new BadDataNodeInfo(toMark.getIpAddr(),
            toMark.getXferPort(), false);
    cluster.getFileSystem().markBadDataNodes(new BadDataNodeInfo[]{normalNode});
    datanodes = datanodeManager.getDatanodes();
    for (DatanodeInfo node : datanodes) {
      Assert.assertFalse(node.isReportedBad());
    }
  }
}
