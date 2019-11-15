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

package org.apache.hadoop.yarn.nodelabels;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.Assert;
import org.junit.Test;

public class TestRMNodeLabelMetrics {

  MetricsSystem ms = DefaultMetricsSystem.instance();

  private void testParitionMetrics(String labelName) throws Exception {
    RMNodeLabelMetrics metrics = RMNodeLabelMetrics.getNodeLabelMetrics(labelName, 2);
    Assert.assertNotNull(ms.getSource(metrics.getMetricsSourceName()));
    Assert.assertEquals(2, metrics.getNumActiveNMs());

    metrics.incrNumActiveNMs();
    Assert.assertTrue(metrics.numActiveNMs.changed());
    Assert.assertEquals(3, metrics.getNumActiveNMs());

    metrics.decrNumActiveNMs();
    Assert.assertTrue(metrics.numActiveNMs.changed());
    Assert.assertEquals(2, metrics.getNumActiveNMs());

    metrics.removeNodeLabelMetrics(labelName);
    Assert.assertNull(ms.getSource(metrics.getMetricsSourceName()));
  }

  @Test
  public void testDefaultPartitionMetrics() throws Exception {
    testParitionMetrics("");
  }

  @Test
  public void testLabeledPartitionMetrics() throws Exception {
    testParitionMetrics("x");
  }

}
