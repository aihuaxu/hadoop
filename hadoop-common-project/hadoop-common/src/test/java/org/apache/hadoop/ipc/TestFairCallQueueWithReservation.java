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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.junit.Test;
import org.mockito.Mockito;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestFairCallQueueWithReservation extends TestCase {
  private FairCallQueueWithReservation<Schedulable> fcq;

  private Schedulable mockCall(String id, int priority) {
    Schedulable mockCall = mock(Schedulable.class);
    UserGroupInformation ugi = mock(UserGroupInformation.class);

    when(ugi.getUserName()).thenReturn(id);
    when(mockCall.getUserGroupInformation()).thenReturn(ugi);
    when(mockCall.getPriorityLevel()).thenReturn(priority);
    when(mockCall.toString()).thenReturn("id=" + id + " priority=" + priority);

    return mockCall;
  }

  private Schedulable mockCall(String id) {
    return mockCall(id, 0);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void setUp() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 3);

    // We config 3 shared queues, and 2 reserved queues with shares [20%, 10%]
    fcq = new FairCallQueueWithReservation<Schedulable>(3,
        new double[]{0.2, 0.1}, 10, "ns", conf);
  }

  /**
   * Validate that the total capacity of all subqueues equals
   * to the maxQueueSize for different configs
   */
  @Test
  public void testTotalCapacityOfSubQueues() {
    Configuration conf = new Configuration();
    FairCallQueueWithReservation<Schedulable> queue;
    queue = new FairCallQueueWithReservation<Schedulable>
        (1, null, 1000, "ns", conf);
    assertEquals(queue.remainingCapacity(), 1000);
    queue = new FairCallQueueWithReservation<Schedulable>
        (4, null, 1000, "ns", conf);
    assertEquals(queue.remainingCapacity(), 1000);
    queue = new FairCallQueueWithReservation<Schedulable>
        (7, null, 1025, "ns", conf);
    assertEquals(queue.remainingCapacity(), 1025);
    queue = new FairCallQueueWithReservation<Schedulable>
        (1, new double[]{0.2}, 1000, "ns", conf);
    assertEquals(queue.remainingCapacity(), 1000);
    queue = new FairCallQueueWithReservation<Schedulable>
        (4, new double[]{0.3, 0.2, 0.1}, 1000, "ns", conf);
    assertEquals(queue.remainingCapacity(), 1000);
    queue = new FairCallQueueWithReservation<Schedulable>
        (7, new double[]{0.5, 0.1}, 1025, "ns", conf);
    assertEquals(queue.remainingCapacity(), 1025);
  }

  @Test
  public void testPrioritization() {
    // Create 10 queues, with 3 reserved queues
    Configuration conf = new Configuration();
    fcq = new FairCallQueueWithReservation<>(7,
        new double[]{0.1, 0.1, 0.1}, 10, "ns", conf);

    List<Schedulable> calls = new ArrayList<Schedulable>();
    for (int i = 0; i < 10; i ++) {
      Schedulable call = mockCall("u", i);
      calls.add(call);
      fcq.add(call);
    }

    final AtomicInteger currentIndex = new AtomicInteger();
    fcq.setMultiplexer(new RpcMultiplexer(){
      @Override
      public int getAndAdvanceCurrentIndex() {
        return currentIndex.get();
      }
    });

    // For shared queue (0~6), if there is no call at a given index,
    // return the next highest priority call available.
    //   v
    //0123456789
    currentIndex.set(3);
    assertSame(calls.get(3), fcq.poll());
    assertSame(calls.get(0), fcq.poll());
    assertSame(calls.get(1), fcq.poll());
    //     v
    //--2-456789
    currentIndex.set(5);
    assertSame(calls.get(5), fcq.poll());
    assertSame(calls.get(2), fcq.poll());
    // For reserved queue (7~9), if there is no call at a given index,
    // return the next
    //        v
    //----4-6789
    currentIndex.set(8);
    assertSame(calls.get(8), fcq.poll());
    assertSame(calls.get(9), fcq.poll());
    assertSame(calls.get(4), fcq.poll());
    //      v
    //------67--
    currentIndex.set(6);
    assertSame(calls.get(6), fcq.poll());
    assertSame(calls.get(7), fcq.poll());
    //----------
    assertNull(fcq.poll());
    assertNull(fcq.poll());
  }

  @SuppressWarnings("unchecked") // for mock reset.
  @Test
  public void testInsertion() throws Exception {
    Configuration conf = new Configuration();
    // 5 queues (3 shared queues + 2 reserved queues), 2 slots each
    fcq = Mockito.spy(new FairCallQueueWithReservation<Schedulable>
        (3, new double[]{0.3, 0.1}, 10, "ns", conf));

    Schedulable p0 = mockCall("a", 0);
    Schedulable p1 = mockCall("b", 1);
    Schedulable p2 = mockCall("c", 2);
    Schedulable p3 = mockCall("d", 3);
    Schedulable p4 = mockCall("e", 4);

    // add to first queue
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(0)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);
    Mockito.reset(fcq);
    // 0:x- 1:-- 2:-- 3:-- 4:--

    // add to second queue.
    Mockito.reset(fcq);
    fcq.add(p1);
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(0)).offerQueue(2, p1);
    Mockito.verify(fcq, times(0)).offerQueue(3, p1);
    Mockito.verify(fcq, times(0)).offerQueue(4, p1);
    // 0:x- 1:x- 2:-- 3:-- 4:--

    // add to first queue.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(0)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);
    // 0:xx 1:x- 2:-- 3:-- 4:--

    // add to first full queue spills over to second.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0);
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);
    // 0:xx 1:xx 2:-- 3:-- 4:--

    // add to second full queue spills over to third.
    Mockito.reset(fcq);
    fcq.add(p1);
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(1)).offerQueue(2, p1);
    Mockito.verify(fcq, times(0)).offerQueue(3, p1);
    Mockito.verify(fcq, times(0)).offerQueue(4, p1);
    // 0:xx 1:xx 2:x- 3:-- 4:--

    // add to first and second full queue spills over to third.
    Mockito.reset(fcq);
    fcq.add(p0);
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(1)).offerQueue(2, p0);
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);
    // 0:xx 1:xx 2:xx 3:-- 4:--

    // adding non-lowest priority with all shared queues full throws a
    // non-disconnecting rpc server exception, although reserved
    // queues are empty.
    Mockito.reset(fcq);
    try {
      fcq.add(p0);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.ERROR);
    }
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(1)).offerQueue(2, p0);
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);

    // adding non-lowest priority with all shared queues full throws a
    // non-disconnecting rpc server exception, although reserved
    // queues are empty.
    Mockito.reset(fcq);
    try {
      fcq.add(p1);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.ERROR);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p1);
    Mockito.verify(fcq, times(1)).offerQueue(1, p1);
    Mockito.verify(fcq, times(1)).offerQueue(2, p1);
    Mockito.verify(fcq, times(0)).offerQueue(3, p1);
    Mockito.verify(fcq, times(0)).offerQueue(4, p1);

    // adding lowest priority with all shared queues full throws a
    // fatal rpc server exception, although reserved
    // queues are empty.
    Mockito.reset(fcq);
    try {
      fcq.add(p2);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.FATAL);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p2);
    Mockito.verify(fcq, times(0)).offerQueue(1, p2);
    Mockito.verify(fcq, times(1)).offerQueue(2, p2);
    Mockito.verify(fcq, times(0)).offerQueue(3, p2);
    Mockito.verify(fcq, times(0)).offerQueue(4, p2);

    // adding to a reserved queue
    Mockito.reset(fcq);
    fcq.add(p3);
    Mockito.verify(fcq, times(0)).offerQueue(0, p3);
    Mockito.verify(fcq, times(0)).offerQueue(1, p3);
    Mockito.verify(fcq, times(0)).offerQueue(2, p3);
    Mockito.verify(fcq, times(1)).offerQueue(3, p3);
    Mockito.verify(fcq, times(0)).offerQueue(4, p3);

    // make it full
    Mockito.reset(fcq);
    fcq.add(p3);
    Mockito.verify(fcq, times(0)).offerQueue(0, p3);
    Mockito.verify(fcq, times(0)).offerQueue(1, p3);
    Mockito.verify(fcq, times(0)).offerQueue(2, p3);
    Mockito.verify(fcq, times(1)).offerQueue(3, p3);
    Mockito.verify(fcq, times(0)).offerQueue(4, p3);

    // adding to a full reserved queue throws a non-disconnecting
    // rpc server exception, although the following reserved queue
    // is emtpy
    Mockito.reset(fcq);
    try {
      fcq.add(p3);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.FATAL);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p3);
    Mockito.verify(fcq, times(0)).offerQueue(1, p3);
    Mockito.verify(fcq, times(0)).offerQueue(2, p3);
    Mockito.verify(fcq, times(1)).offerQueue(3, p3);
    Mockito.verify(fcq, times(0)).offerQueue(4, p3);

    // Same to the other reserved queue
    Mockito.reset(fcq);
    fcq.add(p4);
    Mockito.verify(fcq, times(0)).offerQueue(0, p4);
    Mockito.verify(fcq, times(0)).offerQueue(1, p4);
    Mockito.verify(fcq, times(0)).offerQueue(2, p4);
    Mockito.verify(fcq, times(0)).offerQueue(3, p4);
    Mockito.verify(fcq, times(1)).offerQueue(4, p4);

    Mockito.reset(fcq);
    fcq.add(p4);
    Mockito.verify(fcq, times(0)).offerQueue(0, p4);
    Mockito.verify(fcq, times(0)).offerQueue(1, p4);
    Mockito.verify(fcq, times(0)).offerQueue(2, p4);
    Mockito.verify(fcq, times(0)).offerQueue(3, p4);
    Mockito.verify(fcq, times(1)).offerQueue(4, p4);

    // adding to a full reserved queue throws a non-disconnecting
    // rpc server exception
    Mockito.reset(fcq);
    try {
      fcq.add(p4);
      fail("didn't fail");
    } catch (IllegalStateException ise) {
      checkOverflowException(ise, RpcStatusProto.FATAL);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p4);
    Mockito.verify(fcq, times(0)).offerQueue(1, p4);
    Mockito.verify(fcq, times(0)).offerQueue(2, p4);
    Mockito.verify(fcq, times(0)).offerQueue(3, p4);
    Mockito.verify(fcq, times(1)).offerQueue(4, p4);

    // used to abort what would be a blocking operation.
    Exception stopPuts = new RuntimeException();

    // For shared queues, put should offer to all but last shared queue,
    // only put to last shared queue
    Mockito.reset(fcq);
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any(Schedulable.class));
      fcq.put(p0);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(1)).offerQueue(0, p0);
    Mockito.verify(fcq, times(1)).offerQueue(1, p0);
    Mockito.verify(fcq, times(0)).offerQueue(2, p0); // expect put, not offer.
    Mockito.verify(fcq, times(1)).putQueue(2, p0);
    // No put, no offer for reserved queues
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);
    Mockito.verify(fcq, times(0)).putQueue(3, p0);
    Mockito.verify(fcq, times(0)).putQueue(4, p0);

    // put with lowest shared queue should not offer, just put.
    Mockito.reset(fcq);
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any(Schedulable.class));
      fcq.put(p2);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p2);
    Mockito.verify(fcq, times(0)).offerQueue(1, p2);
    Mockito.verify(fcq, times(0)).offerQueue(2, p2);
    Mockito.verify(fcq, times(1)).putQueue(2, p2);
    // No put, no offer for reserved queues
    Mockito.verify(fcq, times(0)).offerQueue(3, p0);
    Mockito.verify(fcq, times(0)).offerQueue(4, p0);
    Mockito.verify(fcq, times(0)).putQueue(3, p0);
    Mockito.verify(fcq, times(0)).putQueue(4, p0);

    // For reserved queue, just put, no offer to other queues
    Mockito.reset(fcq);
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any(Schedulable.class));
      fcq.put(p3);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p3);
    Mockito.verify(fcq, times(0)).offerQueue(1, p3);
    Mockito.verify(fcq, times(0)).offerQueue(2, p3);
    Mockito.verify(fcq, times(0)).offerQueue(3, p3);
    Mockito.verify(fcq, times(0)).offerQueue(4, p3);
    Mockito.verify(fcq, times(0)).putQueue(0, p3);
    Mockito.verify(fcq, times(0)).putQueue(1, p3);
    Mockito.verify(fcq, times(0)).putQueue(2, p3);
    Mockito.verify(fcq, times(1)).putQueue(3, p3);
    Mockito.verify(fcq, times(0)).putQueue(4, p3);

    // For reserved queue, just put, no offer to other queues
    Mockito.reset(fcq);
    try {
      doThrow(stopPuts).when(fcq).putQueue(anyInt(), any(Schedulable.class));
      fcq.put(p4);
      fail("didn't fail");
    } catch (Exception e) {
      assertSame(stopPuts, e);
    }
    Mockito.verify(fcq, times(0)).offerQueue(0, p4);
    Mockito.verify(fcq, times(0)).offerQueue(1, p4);
    Mockito.verify(fcq, times(0)).offerQueue(2, p4);
    Mockito.verify(fcq, times(0)).offerQueue(3, p4);
    Mockito.verify(fcq, times(0)).offerQueue(4, p4);
    Mockito.verify(fcq, times(0)).putQueue(0, p4);
    Mockito.verify(fcq, times(0)).putQueue(1, p4);
    Mockito.verify(fcq, times(0)).putQueue(2, p4);
    Mockito.verify(fcq, times(0)).putQueue(3, p4);
    Mockito.verify(fcq, times(1)).putQueue(4, p4);
  }

  private void checkOverflowException(Exception ex, RpcStatusProto status) {
    // should be an overflow exception
    assertTrue(ex.getClass().getName() + " != CallQueueOverflowException",
        ex instanceof CallQueueManager.CallQueueOverflowException);
    IOException ioe = ((CallQueueManager.CallQueueOverflowException)ex).getCause();
    assertNotNull(ioe);
    assertTrue(ioe.getClass().getName() + " != RpcServerException",
        ioe instanceof RpcServerException);
    RpcServerException rse = (RpcServerException)ioe;
    // check error/fatal status and if it embeds a retriable ex.
    assertEquals(status, rse.getRpcStatusProto());
    assertTrue(rse.getClass().getName() + " != RetriableException",
        rse.getCause() instanceof RetriableException);
  }

  @Test
  public void testPollReturnsNullWhenEmpty() {
    assertNull(fcq.poll());
  }

  @Test
  public void testPollReturnsTopCallWhenNotEmpty() {
    Schedulable call = mockCall("c");
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.poll());

    // Poll took it out so the fcq is empty
    assertEquals(0, fcq.size());
  }

  @Test
  public void testOfferSucceeds() {
    for (int i = 0; i < 2; i++) {
      // We can fit 2 calls
      assertTrue(fcq.offer(mockCall("c")));
    }

    assertEquals(2, fcq.size());
  }

  @Test
  public void testOfferFailsWhenFull() {
    for (int i = 0; i < 2; i++) { assertTrue(fcq.offer(mockCall("c"))); }

    assertFalse(fcq.offer(mockCall("c"))); // It's full

    assertEquals(2, fcq.size());
  }

  @Test
  public void testOfferSucceedsWhenScheduledLowPriority() {
    // Scheduler will schedule into queue 0 x 2, then queue 1 x 2,
    // then queue 2 x 2, then queue 3 x 2, then queue 4 x 2
    int mockedPriorities[] = {0, 0, 1, 1, 2, 2, 3, 3, 4, 4};
    for (int i = 0; i < 9; i++) {
      assertTrue(fcq.offer(mockCall("c", mockedPriorities[i])));
    }

    assertTrue(fcq.offer(mockCall("c", mockedPriorities[9])));

    assertEquals(10, fcq.size());
  }

  @Test
  public void testPeekNullWhenEmpty() {
    assertNull(fcq.peek());
  }

  @Test
  public void testPeekNonDestructive() {
    Schedulable call = mockCall("c", 0);
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.peek());
    assertEquals(call, fcq.peek()); // Non-destructive
    assertEquals(1, fcq.size());
  }

  @Test
  public void testPeekPointsAtHead() {
    Schedulable call = mockCall("c", 0);
    Schedulable next = mockCall("b", 0);
    fcq.offer(call);
    fcq.offer(next);

    assertEquals(call, fcq.peek()); // Peek points at the head
  }

  @Test
  public void testPollTimeout() throws InterruptedException {
    assertNull(fcq.poll(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testPollSuccess() throws InterruptedException {
    Schedulable call = mockCall("c", 0);
    assertTrue(fcq.offer(call));

    assertEquals(call, fcq.poll(10, TimeUnit.MILLISECONDS));

    assertEquals(0, fcq.size());
  }

  @Test
  public void testOfferTimeout() throws InterruptedException {
    for (int i = 0; i < 2; i++) {
      assertTrue(fcq.offer(mockCall("c"), 10, TimeUnit.MILLISECONDS));
    }

    assertFalse(fcq.offer(mockCall("e"), 10, TimeUnit.MILLISECONDS)); // It's full

    assertEquals(2, fcq.size());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDrainTo() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);
    FairCallQueueWithReservation<Schedulable> fcq2 =
        new FairCallQueueWithReservation<Schedulable>(
            2, new double[]{0.3, 0.2}, 10, "ns", conf);

    // 2 calls in queue 0, 2 calls in queue 2, 2 calls in queue 3
    for (int i = 0; i < 2; i++) {
      fcq.offer(mockCall("c"));
    }
    for (int i = 0; i < 2; i++) {
      fcq.offer(mockCall("c", 2));
    }
    for (int i = 0; i < 2; i++) {
      fcq.offer(mockCall("c", 3));
    }

    fcq.drainTo(fcq2);

    assertEquals(0, fcq.size());
    assertEquals(6, fcq2.size());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDrainToWithLimit() {
    Configuration conf = new Configuration();
    conf.setInt("ns." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 2);
    FairCallQueueWithReservation<Schedulable> fcq2 =
        new FairCallQueueWithReservation<Schedulable>(
            2, new double[]{0.3, 0.2}, 10, "ns", conf);

    // 4 calls in queue 0, 2 calls in queue 2, 2 calls in queue 3
    for (int i = 0; i < 2; i++) {
      assertTrue(fcq.offer(mockCall("c")));
    }
    for (int i = 0; i < 2; i++) {
      assertTrue(fcq.offer(mockCall("c", 2)));
    }
    for (int i = 0; i < 2; i++) {
      assertTrue(fcq.offer(mockCall("c", 3)));
    }

    fcq.drainTo(fcq2, 2);

    assertEquals(4, fcq.size());
    assertEquals(2, fcq2.size());
  }

  @Test
  public void testInitialRemainingCapacity() {
    assertEquals(10, fcq.remainingCapacity());
  }

  @Test
  public void testFirstQueueFullRemainingCapacity() {
    while (fcq.offer(mockCall("c"))) ; // Queue 0 will fill up first, then queue 1

    assertEquals(8, fcq.remainingCapacity());
  }

  @Test
  public void testAllQueuesFullRemainingCapacity() {
    int[] mockedPriorities = {0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 0};
    int i = 0;
    while (fcq.offer(mockCall("c", mockedPriorities[i++]))) ;

    assertEquals(0, fcq.remainingCapacity());
    assertEquals(10, fcq.size());
  }

  @Test
  public void testQueuesPartialFilledRemainingCapacity() {
    int[] mockedPriorities = {0, 1, 0, 1, 0};
    for (int i = 0; i < 5; i++) { fcq.offer(mockCall("c", mockedPriorities[i])); }

    assertEquals(6, fcq.remainingCapacity());
    assertEquals(4, fcq.size());
  }

  @Test
  public void testTakeRemovesCall() throws InterruptedException {
    Schedulable call = mockCall("c");
    fcq.offer(call);

    assertEquals(call, fcq.take());
    assertEquals(0, fcq.size());
  }

  @Test
  public void testTakeTriesNextQueue() throws InterruptedException {
    // A mux which only draws from q 0
    RpcMultiplexer q0mux = mock(RpcMultiplexer.class);
    when(q0mux.getAndAdvanceCurrentIndex()).thenReturn(0);
    fcq.setMultiplexer(q0mux);

    // Make a FCQ filled with calls in q 1 but empty in q 0
    Schedulable call = mockCall("c", 1);
    fcq.put(call);

    // Take from q1 even though mux said q0, since q0 empty
    assertEquals(call, fcq.take());
    assertEquals(0, fcq.size());
  }

  @Test
  public void testFairCallQueueWithReservationMXBean() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName = new ObjectName(
        "Hadoop:service=ns,name=FairCallQueueWithReservation");

    Schedulable call0 = mockCall("c", 0);
    Schedulable call4 = mockCall("c", 4);
    fcq.put(call0);
    fcq.put(call4);
    int[] queueSizes = (int[]) mbs.getAttribute(mxbeanName, "QueueSizes");
    assertEquals(1, queueSizes[0]);
    assertEquals(0, queueSizes[1]);
    assertEquals(0, queueSizes[2]);
    assertEquals(0, queueSizes[3]);
    assertEquals(1, queueSizes[4]);
    fcq.take();
    queueSizes = (int[]) mbs.getAttribute(mxbeanName, "QueueSizes");
    assertEquals(0, queueSizes[0]);
    assertEquals(0, queueSizes[1]);
    assertEquals(0, queueSizes[2]);
    assertEquals(0, queueSizes[3]);
    assertEquals(1, queueSizes[4]);
    fcq.take();
    queueSizes = (int[]) mbs.getAttribute(mxbeanName, "QueueSizes");
    assertEquals(0, queueSizes[0]);
    assertEquals(0, queueSizes[1]);
    assertEquals(0, queueSizes[2]);
    assertEquals(0, queueSizes[3]);
    assertEquals(0, queueSizes[4]);
  }
}
