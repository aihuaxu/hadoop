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

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT;

public class TestDataNodeComparators {
  private static final long staleInterval =
          DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT * 2; // 1 min
  private static final Random random = new Random();

  private DatanodeInfo[] generateRandomDatanodes() {
    List<DatanodeInfo> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      list.add(generateDatanodeInfo(random.nextBoolean(), random.nextBoolean(),
              random.nextBoolean()));
    }
    Collections.shuffle(list);
    return list.toArray(new DatanodeInfo[0]);
  }

  private DatanodeInfo generateDatanodeInfo(boolean decommissioned,
          boolean isStale, boolean reportedBad) {
    DatanodeID datanodeID = new DatanodeID("127.0.0.1", "localhost",
            UUID.randomUUID().toString(), genRandomPort(), genRandomPort(),
            genRandomPort(), genRandomPort());
    DatanodeInfo datanodeInfo = new DatanodeInfo(datanodeID);
    if (decommissioned) {
      datanodeInfo.setDecommissioned();
    }
    if (isStale) {
      datanodeInfo.setLastUpdateMonotonic(Time.monotonicNow() - staleInterval * 2);
    } else {
      datanodeInfo.setLastUpdateMonotonic(Time.monotonicNow());
    }
    if (reportedBad) {
      datanodeInfo.setReportedBad(true);
    }
    return datanodeInfo;
  }

  private int genRandomPort() {
    return random.nextInt(1000) + 8000;
  }

  private boolean checkSortedDataNodes(DatanodeInfo[] nodes, boolean checkStale) {
    for (int i = 0; i < nodes.length - 1; i++) {
      DatanodeInfo current = nodes[i];
      DatanodeInfo next = nodes[i+1];
      if (current.isDecommissioned() && !next.isDecommissioned()) {
        return false;
      }
      if (!current.isDecommissioned() && !next.isDecommissioned() &&
              current.isReportedBad() && !next.isReportedBad()) {
        return false;
      }
      if (checkStale && !next.isDecommissioned() && !next.isReportedBad()) {
        if (current.isStale(staleInterval) && !next.isStale(staleInterval)) {
          return false;
        }
      }
    }
    return true;
  }

  @Test
  public void testDecomComparator() {
    DatanodeInfo[] nodes = generateRandomDatanodes();
    Arrays.sort(nodes, DFSUtil.DECOM_COMPARATOR);
    Assert.assertTrue(checkSortedDataNodes(nodes, false));
  }

  @Test
  public void testDecomStaleComparator() {
    DatanodeInfo[] nodes = generateRandomDatanodes();
    Arrays.sort(nodes, new DFSUtil.DecomStaleComparator(staleInterval));
    Assert.assertTrue(checkSortedDataNodes(nodes, true));
  }
}
