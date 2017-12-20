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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.CallQueueManager.CallQueueOverflowException;
import org.apache.hadoop.metrics2.util.MBeans;

import java.lang.ref.WeakReference;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An extension to {@link FairCallQueue} to add some reserved queues.
 * In {@link FairCallQueueWithReservation}, there will be two sets of queues:
 *  - Shared queues: refer to the previous queues implemented in
 *    {@link FairCallQueue}. RPC requests are placed into different priority
 *    queues based on call volumes users made in the past.
 *  - Reserved queues: refer to the set of queues dedicated for a set of
 *    special users. Each special user will have one reserved queue, and all
 *    RPC requests from this user will be placed into corresponding reserved
 *    queue, no matter the call volume. Each reserved queue has a guaranteed
 *    reserved share (like 10%) of RPC processing resources.
 */
public class FairCallQueueWithReservation<E extends Schedulable>
    extends AbstractQueue<E> implements BlockingQueue<E> {

  public static final Log LOG =
      LogFactory.getLog(FairCallQueueWithReservation.class);

  /* The queues (including both shared queues which come first
   and reserved queues) */
  private final ArrayList<BlockingQueue<E>> queues;
  private final int numSharedQueues;
  private final int numReservedQueues;
  private final int numQueues;

  /* Track available permits for scheduled objects.  All methods that will
   * mutate a subqueue must acquire or release a permit on the semaphore.
   * A semaphore is much faster than an exclusive lock because producers do
   * not contend with consumers and consumers do not block other consumers
   * while polling.
   */
  private final Semaphore semaphore = new Semaphore(0);
  private void signalNotEmpty() {
    semaphore.release();
  }

  /* Multiplexer picks which queue to draw from */
  private RpcMultiplexer multiplexer;

  /* Statistic tracking */
  private final ArrayList<AtomicLong> overflowedCalls;

  public FairCallQueueWithReservation(int sharedPriorityLevels,
      double[] reservedShares, int capacity, String ns, Configuration conf) {
    // Shared queues
    if (sharedPriorityLevels < 1) {
      throw new IllegalArgumentException("Number of Shared Priority Levels " +
          "must be at least 1");
    }
    this.numSharedQueues = sharedPriorityLevels;

    // Reserved queues
    this.numReservedQueues = reservedShares == null ? 0 : reservedShares.length;

    this.numQueues = numSharedQueues + numReservedQueues;
    LOG.info("FairCallQueueWithReservation is in use with " + sharedPriorityLevels +
        " shared queues and " + numReservedQueues + " reserved queues," +
        " with total capacity of " + capacity + ". ");

    // Config queues
    this.queues = new ArrayList<BlockingQueue<E>>(numQueues);
    this.overflowedCalls = new ArrayList<AtomicLong>(numQueues);
    int queueCapacity = capacity / numQueues;
    int capacityForFirstQueue = queueCapacity + (capacity % numQueues);
    for(int i = 0; i < numQueues; i++) {
      if (i == 0) {
        this.queues.add(new LinkedBlockingQueue<E>(capacityForFirstQueue));
      } else {
        this.queues.add(new LinkedBlockingQueue<E>(queueCapacity));
      }
      this.overflowedCalls.add(new AtomicLong(0));
    }

    this.multiplexer = new WeightedRoundRobinMultiplexer(numSharedQueues,
        reservedShares, ns, conf);

    // Make this the active source of metrics
    MetricsProxy mp = MetricsProxy.getInstance(ns);
    mp.setDelegate(this);
  }

  /**
   * Returns an element first non-empty queue equal to the priority returned
   * by the multiplexer or scans from highest to lowest priority queue.
   *
   * Caller must always acquire a semaphore permit before invoking.
   *
   * @return the first non-empty queue with less priority, or null if
   * everything was empty
   */
  private E removeNextElement() {
    int priority = multiplexer.getAndAdvanceCurrentIndex();
    E e = queues.get(priority).poll();

    if (e == null) {
      // For shared queues, we start from the highest priority queue
      // (startIdx=0) to find another candidate.
      // For reserved queues, we start the current point as there is no
      // high/low priorities among reserved queues.
      int startIdx = isForReservedQueue(priority) ? priority : 0;
      for (int i = 0; e == null && i < numQueues; i++) {
        int idx = (i + startIdx) % numQueues; // offset and wrap around
        e = queues.get(idx).poll();
      }
    }

    // guaranteed to find an element if caller acquired permit.
    assert e != null : "consumer didn't acquire semaphore!";
    return e;
  }

  /* AbstractQueue and BlockingQueue methods */

  /**
   * Add, put, and offer follow the same pattern:
   * 1. Get the assigned priorityLevel from the call by scheduler
   * 2. Get the nth sub-queue matching this priorityLevel
   * 3. delegate the call to this sub-queue.
   *
   * But differ in how they handle overflow for shared queues:
   * - Add will move on to the next queue, throw on last queue overflow
   * - Put will move on to the next queue, block on last queue overflow
   * - Offer does not attempt other queues on overflow
   *
   * Shared queues handle differently in overflow:
   * - Add will not move to the next queue, and directly throw exception
   * - Put will not move to the next queue, and will be blocked on current
   *   queue
   * - Offer same as shared queues
   */

  @Override
  public boolean add(E e) {
    final int priorityLevel = e.getPriorityLevel();
    if (isForReservedQueue(priorityLevel)) {
      // Try offering to the corresponding reserved queue only
      if (!offerReservedQueue(priorityLevel, e)) {
        throw CallQueueOverflowException.DISCONNECT;
      }
    } else {
      // try offering to all shared queues
      if (!offerSharedQueues(priorityLevel, e, true)) {
        // only disconnect the lowest priority users that overflow the queue.
        throw (priorityLevel == numSharedQueues - 1)
            ? CallQueueOverflowException.DISCONNECT
            : CallQueueOverflowException.KEEPALIVE;
      }
    }
    return true;
  }

  @Override
  public void put(E e) throws InterruptedException {
    final int priorityLevel = e.getPriorityLevel();
    if (isForReservedQueue(priorityLevel)) {
      putQueue(priorityLevel, e);
    } else {
      // try offering to all but last queue, put on last.
      if (!offerSharedQueues(priorityLevel, e, false)) {
        putQueue(numSharedQueues - 1, e);
      }
    }
  }

  @Override
  public boolean offer(E e, long timeout, TimeUnit unit)
      throws InterruptedException {
    int priorityLevel = e.getPriorityLevel();
    BlockingQueue<E> q = this.queues.get(priorityLevel);
    boolean ret = q.offer(e, timeout, unit);
    if (ret) {
      signalNotEmpty();
    }
    return ret;
  }

  @Override
  public boolean offer(E e) {
    int priorityLevel = e.getPriorityLevel();
    BlockingQueue<E> q = this.queues.get(priorityLevel);
    boolean ret = q.offer(e);
    if (ret) {
      signalNotEmpty();
    }
    return ret;
  }

  /**
   * Offer the element to queue of a specific priority.
   * @param priority - queue priority
   * @param e - element to add
   * @return boolean if added to the given queue
   */
  @VisibleForTesting
  boolean offerQueue(int priority, E e) {
    boolean ret = queues.get(priority).offer(e);
    if (ret) {
      signalNotEmpty();
    }
    return ret;
  }

  /**
   * Offer the element to shared queue of the given or lower priority.
   * @param priority - starting queue priority
   * @param e - element to add
   * @param includeLast - whether to attempt last queue
   * @return boolean if added to a queue
   */
  private boolean offerSharedQueues(int priority, E e, boolean includeLast) {
    int lastPriority = numSharedQueues - (includeLast ? 1 : 2);
    for (int i=priority; i <= lastPriority; i++) {
      if (offerQueue(i, e)) {
        return true;
      }
      // Update stats
      overflowedCalls.get(i).getAndIncrement();
    }
    return false;
  }

  /**
   * Offer the element to a reserved queue of the given priority
   * @param priority - corresponding queue to add the element
   * @param e - element to add
   * @return true if added to a queue
   */
  private boolean offerReservedQueue(int priority, E e) {
    if (offerQueue(priority, e)) {
      return true;
    }
    // Update stats
    overflowedCalls.get(priority).getAndIncrement();
    return false;
  }

  /**
   * Put the element in a queue of a specific priority.
   * @param priority - queue priority
   * @param e - element to add
   */
  @VisibleForTesting
  void putQueue(int priority, E e) throws InterruptedException {
    queues.get(priority).put(e);
    signalNotEmpty();
  }

  @Override
  public E take() throws InterruptedException {
    semaphore.acquire();
    return removeNextElement();
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    return semaphore.tryAcquire(timeout, unit) ? removeNextElement() : null;
  }

  /**
   * poll() provides no strict consistency: it is possible for poll to return
   * null even though an element is in the queue.
   */
  @Override
  public E poll() {
    return semaphore.tryAcquire() ? removeNextElement() : null;
  }

  /**
   * Peek, like poll, provides no strict consistency.
   */
  @Override
  public E peek() {
    E e = null;
    for (int i=0; e == null && i < queues.size(); i++) {
      e = queues.get(i).peek();
    }
    return e;
  }

  /**
   * Size returns the sum of all sub-queue sizes, so it may be greater than
   * capacity.
   * Note: size provides no strict consistency, and should not be used to
   * control queue IO.
   */
  @Override
  public int size() {
    return semaphore.availablePermits();
  }

  /**
   * Iterator is not implemented, as it is not needed.
   */
  @Override
  public Iterator<E> iterator() {
    throw new NotImplementedException();
  }

  /**
   * drainTo defers to each sub-queue. Note that draining from a FairCallQueue
   * to another FairCallQueue will likely fail, since the incoming calls
   * may be scheduled differently in the new FairCallQueue. Nonetheless this
   * method is provided for completeness.
   */
  @Override
  public int drainTo(Collection<? super E> c, int maxElements) {
    // initially take all permits to stop consumers from modifying queues
    // while draining.  will restore any excess when done draining.
    final int permits = semaphore.drainPermits();
    final int numElements = Math.min(maxElements, permits);
    int numRemaining = numElements;
    for (int i=0; numRemaining > 0 && i < queues.size(); i++) {
      numRemaining -= queues.get(i).drainTo(c, numRemaining);
    }
    int drained = numElements - numRemaining;
    if (permits > drained) { // restore unused permits.
      semaphore.release(permits - drained);
    }
    return drained;
  }

  @Override
  public int drainTo(Collection<? super E> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  /**
   * Returns maximum remaining capacity. This does not reflect how much you can
   * ideally fit in this FairCallQueue, as that would depend on the scheduler's
   * decisions.
   */
  @Override
  public int remainingCapacity() {
    int sum = 0;
    for (BlockingQueue<E> q : this.queues) {
      sum += q.remainingCapacity();
    }
    return sum;
  }

  /**
   * Reserved queues' indices start from {@param numSharedQueues}
   */
  private boolean isForReservedQueue(int priority) {
    return priority >= numSharedQueues;
  }

  /**
   * MetricsProxy is a singleton because we may init multiple
   * FairCallQueueWithReservations, but the metrics system cannot
   * unregister beans cleanly.
   */
  static final class MetricsProxy implements FairCallQueueMXBean {
    // One singleton per namespace
    private static final HashMap<String, MetricsProxy> INSTANCES =
        new HashMap<String, MetricsProxy>();

    // Weakref for delegate, so we don't retain it forever if it can be GC'd
    private WeakReference<FairCallQueueWithReservation<? extends Schedulable>>
        delegate;

    // Keep track of how many objects we registered
    private int revisionNumber = 0;

    private MetricsProxy(String namespace) {
      MBeans.register(namespace, "FairCallQueueWithReservation", this);
    }

    public static synchronized MetricsProxy getInstance(String namespace) {
      MetricsProxy mp = INSTANCES.get(namespace);
      if (mp == null) {
        // We must create one
        mp = new MetricsProxy(namespace);
        INSTANCES.put(namespace, mp);
      }
      return mp;
    }

    public void setDelegate(
        FairCallQueueWithReservation<? extends Schedulable> obj) {
      this.delegate
          = new WeakReference<FairCallQueueWithReservation<? extends Schedulable>>(obj);
      this.revisionNumber++;
    }

    @Override
    public int[] getQueueSizes() {
      FairCallQueueWithReservation<? extends Schedulable> obj = this.delegate.get();
      if (obj == null) {
        return new int[]{};
      }

      return obj.getQueueSizes();
    }

    @Override
    public long[] getOverflowedCalls() {
      FairCallQueueWithReservation<? extends Schedulable> obj = this.delegate.get();
      if (obj == null) {
        return new long[]{};
      }

      return obj.getOverflowedCalls();
    }

    @Override public int getRevision() {
      return revisionNumber;
    }
  }

  // FairCallQueueMXBean
  public int[] getQueueSizes() {
    int numQueues = queues.size();
    int[] sizes = new int[numQueues];
    for (int i=0; i < numQueues; i++) {
      sizes[i] = queues.get(i).size();
    }
    return sizes;
  }

  public long[] getOverflowedCalls() {
    int numQueues = queues.size();
    long[] calls = new long[numQueues];
    for (int i=0; i < numQueues; i++) {
      calls[i] = overflowedCalls.get(i).get();
    }
    return calls;
  }

  @VisibleForTesting
  public void setMultiplexer(RpcMultiplexer newMux) {
    this.multiplexer = newMux;
  }
}
