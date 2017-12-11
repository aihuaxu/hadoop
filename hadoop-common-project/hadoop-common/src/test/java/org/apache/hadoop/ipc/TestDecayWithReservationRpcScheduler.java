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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDecayWithReservationRpcScheduler {
  private Schedulable mockCall(String id) {
    Schedulable mockCall = mock(Schedulable.class);
    UserGroupInformation ugi = mock(UserGroupInformation.class);

    when(ugi.getUserName()).thenReturn(id);
    when(mockCall.getUserGroupInformation()).thenReturn(ugi);

    return mockCall;
  }

  private DecayWithReservationRpcScheduler scheduler;

  @Test(expected=IllegalArgumentException.class)
  public void testNegativeScheduler() {
    scheduler = new DecayWithReservationRpcScheduler(
        -1, null, "", new Configuration());
  }

  @Test(expected=IllegalArgumentException.class)
  public void testZeroScheduler() {
    scheduler = new DecayWithReservationRpcScheduler(
        0, null, "", new Configuration());
  }

  @Test
  public void testPriorityWithoutReservedShare() throws Exception {
    Configuration conf = new Configuration();
    final String namespace = "ns";
    conf.set(namespace + "." + DecayRpcScheduler
        .IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY, "99999999"); // Never flush
    conf.set(namespace + "." + DecayRpcScheduler
        .IPC_DECAYSCHEDULER_THRESHOLDS_KEY, "25, 50, 75");
    scheduler = new DecayWithReservationRpcScheduler(4, null, namespace, conf);

    assertEquals(0, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("B")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("B")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("C")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("C")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("A")));
  }

  @Test
  public void testPriorityWithReservedShare() throws Exception {
    Configuration conf = new Configuration();
    final String namespace = "ns";
    conf.set(namespace + "." + DecayRpcScheduler
        .IPC_SCHEDULER_DECAYSCHEDULER_PERIOD_KEY, "99999999"); // Never flush
    conf.set(namespace + "." + DecayRpcScheduler
        .IPC_DECAYSCHEDULER_THRESHOLDS_KEY, "25, 50, 75");

    // Put two reserved users (D, E)
    String[] reservedUsers = {"D", "E"};
    scheduler = new DecayWithReservationRpcScheduler(
        4, reservedUsers, namespace, conf);

    assertEquals(0, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("B")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("B")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("C")));
    assertEquals(0, scheduler.getPriorityLevel(mockCall("C")));
    assertEquals(4, scheduler.getPriorityLevel(mockCall("D")));
    assertEquals(4, scheduler.getPriorityLevel(mockCall("D")));
    assertEquals(5, scheduler.getPriorityLevel(mockCall("E")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(1, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(2, scheduler.getPriorityLevel(mockCall("A")));
    assertEquals(4, scheduler.getPriorityLevel(mockCall("D")));
    assertEquals(5, scheduler.getPriorityLevel(mockCall("E")));
  }
}