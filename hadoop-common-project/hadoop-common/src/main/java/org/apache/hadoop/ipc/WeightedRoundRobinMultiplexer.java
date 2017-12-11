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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Determines which queue to start reading from, occasionally drawing from
 * low-priority queues in order to prevent starvation. Given the pull pattern
 * [9, 4, 1] for 3 queues:
 *
 * The cycle is (a minimum of) 9+4+1=14 reads.
 * Queue 0 is read (at least) 9 times
 * Queue 1 is read (at least) 4 times
 * Queue 2 is read (at least) 1 time
 * Repeat
 *
 * There may be more reads than the minimum due to race conditions. This is
 * allowed by design for performance reasons.
 */
public class WeightedRoundRobinMultiplexer implements RpcMultiplexer {
  // Config keys
  public static final String IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY =
    "faircallqueue.multiplexer.weights";

  public static final Log LOG =
    LogFactory.getLog(WeightedRoundRobinMultiplexer.class);

  private final int numQueues; // The number of queues under our provisioning

  private final AtomicInteger currentQueueIndex; // Current queue we're serving
  private final AtomicInteger requestsLeft; // Number of requests left for this queue

  private int[] queueWeights; // The weights for each queue

  public WeightedRoundRobinMultiplexer(int aNumSharedQueues, String ns,
      Configuration conf) {
    this(aNumSharedQueues, null, ns, conf);
  }

  public WeightedRoundRobinMultiplexer(int aNumSharedQueues,
      double[] aNumReservedShares, String ns, Configuration conf) {
    if (aNumSharedQueues <= 0) {
      throw new IllegalArgumentException("Requested shared queues (" +
          aNumSharedQueues + ") must be greater than zero.");
    }

    int numReservedQueues =
        aNumReservedShares == null ? 0 : aNumReservedShares.length;
    this.numQueues = aNumSharedQueues + numReservedQueues;
    LOG.info("WeightRoundRobinMultiplexer is configured with " +
        aNumSharedQueues + " shared queues and " + numReservedQueues +
        " reserved queues.");

    // Set the weights for shared queues
    int[] sharedQueueWeights = conf.getInts(ns + "." +
        IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY);

    if (sharedQueueWeights.length == 0) {
      sharedQueueWeights = getDefaultQueueWeights(aNumSharedQueues);
    } else if (sharedQueueWeights.length != aNumSharedQueues) {
      throw new IllegalArgumentException(ns + "." +
        IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY + " must specify exactly " +
        aNumSharedQueues + " weights: one for each priority level.");
    }

    // Set the weights for reserved queues
    int[] reservedQueueWeights = null;
    if (numReservedQueues > 0) {
      int totalSharedWeight = 0;
      for (int weight : sharedQueueWeights) {
        totalSharedWeight += weight;
      }
      double totalReservedShare = 0.0;
      for (double share : aNumReservedShares) {
        totalReservedShare += share;
      }
      int totalWeight =
          (int) (Math.ceil(totalSharedWeight / (1 - totalReservedShare)));
      reservedQueueWeights = new int[numReservedQueues];
      for (int i = 0; i < numReservedQueues; i ++) {
        reservedQueueWeights[i] =
            (int) Math.ceil(totalWeight * aNumReservedShares[i]);
      }
      LOG.info("Reserved queues have shares " +
          Arrays.toString(aNumReservedShares) +
          ", and their corresponding weights are " +
          Arrays.toString(reservedQueueWeights) + ".");
    }

    // Merge two sets of weights together
    if (numReservedQueues > 0) {
      this.queueWeights = new int[this.numQueues];
      System.arraycopy(sharedQueueWeights, 0,
          queueWeights, 0, aNumSharedQueues);
      System.arraycopy(reservedQueueWeights, 0,
          queueWeights, aNumSharedQueues, numReservedQueues);
    } else {
      this.queueWeights = sharedQueueWeights;
    }

    this.currentQueueIndex = new AtomicInteger(0);
    this.requestsLeft = new AtomicInteger(this.queueWeights[0]);

    LOG.info("WeightedRoundRobinMultiplexer is being used.");
  }

  /**
   * Creates default weights for each queue. The weights are 2^N.
   */
  private int[] getDefaultQueueWeights(int aNumQueues) {
    int[] weights = new int[aNumQueues];

    int weight = 1; // Start low
    for(int i = aNumQueues - 1; i >= 0; i--) { // Start at lowest queue
      weights[i] = weight;
      weight *= 2; // Double every iteration
    }
    return weights;
  }

  /**
   * Move to the next queue.
   */
  private void moveToNextQueue() {
    int thisIdx = this.currentQueueIndex.get();

    // Wrap to fit in our bounds
    int nextIdx = (thisIdx + 1) % this.numQueues;

    // Set to next index: once this is called, requests will start being
    // drawn from nextIdx, but requestsLeft will continue to decrement into
    // the negatives
    this.currentQueueIndex.set(nextIdx);

    // Finally, reset requestsLeft. This will enable moveToNextQueue to be
    // called again, for the new currentQueueIndex
    this.requestsLeft.set(this.queueWeights[nextIdx]);
  }

  /**
   * Advances the index, which will change the current index
   * if called enough times.
   */
  private void advanceIndex() {
    // Since we did read, we should decrement
    int requestsLeftVal = this.requestsLeft.decrementAndGet();

    // Strict compare with zero (instead of inequality) so that if another
    // thread decrements requestsLeft, only one thread will be responsible
    // for advancing currentQueueIndex
    if (requestsLeftVal == 0) {
      // This is guaranteed to be called exactly once per currentQueueIndex
      this.moveToNextQueue();
    }
  }

  /**
   * Gets the current index. Should be accompanied by a call to
   * advanceIndex at some point.
   */
  private int getCurrentIndex() {
    return this.currentQueueIndex.get();
  }

  /**
   * Use the mux by getting and advancing index.
   */
  public int getAndAdvanceCurrentIndex() {
    int idx = this.getCurrentIndex();
    this.advanceIndex();
    return idx;
  }

}
