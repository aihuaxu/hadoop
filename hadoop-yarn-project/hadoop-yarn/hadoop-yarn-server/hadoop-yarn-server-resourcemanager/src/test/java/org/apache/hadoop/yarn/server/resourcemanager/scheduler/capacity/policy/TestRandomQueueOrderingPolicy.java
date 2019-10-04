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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.RandomQueueOrderingPolicy.RandomIterator;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests {@link RandomQueueOrderingPolicy}.
 */
public class TestRandomQueueOrderingPolicy {

  @Test
  public void testUtilizationOrdering() {
    RandomQueueOrderingPolicy policy = new RandomQueueOrderingPolicy();
    CSQueue q1 = mock(CSQueue.class);
    CSQueue q2 = mock(CSQueue.class);
    CSQueue q3 = mock(CSQueue.class);
    List<CSQueue> queues = Arrays.asList(q1, q2, q3);
    policy.setQueues(queues);
    RandomIterator<CSQueue> itr = (RandomIterator<CSQueue>)
        policy.getAssignmentIterator(null);
    Random r = mock(Random.class);
    when(r.nextInt(eq(3))).thenReturn(1);
    when(r.nextInt(eq(2))).thenReturn(0);
    when(r.nextInt(eq(1))).thenReturn(0);
    itr.setRandom(r);
    List<CSQueue> actual = new ArrayList<>();
    while (itr.hasNext()) {
      actual.add(itr.next());
    }
    Assert.assertEquals(3, actual.size());
    Assert.assertEquals(q2, actual.get(0));
    Assert.assertEquals(q1, actual.get(1));
    Assert.assertEquals(q3, actual.get(2));
  }
}
