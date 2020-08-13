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

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The decay RPC scheduler which also supports reservation.
 * For RPC calls coming from reserved users, scheduler will manage them in dedicated queues;
 * For other calls, it follows the DecayRpcScheduler implementation.
 */
public class DecayWithReservationRpcScheduler extends DecayRpcScheduler {
  public static final Logger LOG =
      LoggerFactory.getLogger(DecayWithReservationRpcScheduler.class);

  private final int numLevels;
  private final int numSharedLevels;
  private final int numReservedLevels;
  private final Map<String, Integer> reservedUserPriorities;

  public DecayWithReservationRpcScheduler(int numSharedLevels,
      String[] reservedUsers, String ns, Configuration conf) {
    super(numSharedLevels, ns, conf);
    this.numSharedLevels = numSharedLevels;
    this.numReservedLevels = reservedUsers == null ? 0 : reservedUsers.length;
    this.numLevels = numSharedLevels + numReservedLevels;
    this.reservedUserPriorities = new HashMap<String, Integer>();

    // Set level/priority for each reserved user
    for (int i = 0; i < numReservedLevels; i++) {
      reservedUserPriorities.put(reservedUsers[i], i + numSharedLevels);
    }

    LOG.info("DecayWithReservationRpcScheduler is initialized with " +
        numSharedLevels + " shared levels and " + numReservedLevels +
        " reserved levels. Reserved user priorities: " +
        reservedUserPriorities + ".");
  }

  @Override
  public int getPriorityLevel(Schedulable obj) {
    // First get the identity
    String identity = getIdentityProvider().makeIdentity(obj);
    if (identity == null) {
      // Identity provider did not handle this
      identity = DECAYSCHEDULER_UNKNOWN_IDENTITY;
    }

    // Check whether the user is in the reservation list
    if (reservedUserPriorities.containsKey(identity)) {
      return reservedUserPriorities.get(identity);
    }

    // If not, follow the DecayRpcScheduler approach
    return getPriorityLevel(identity);
  }

  @Override
  public boolean shouldBackOff(Schedulable obj) {
    boolean backOff = false;
    if (isBackOffByResponseTimeEnabled()) {
      int priorityLevel = obj.getPriorityLevel();
      if (isReservedLevel(priorityLevel)) {
        // Now we don't backoff requests coming from reserved users.
        // TODO (T1165853): we may revisit this later on
        if (LOG.isDebugEnabled()) {
          LOG.debug("Current Caller: {} Priority: {} ",
              obj.getUserGroupInformation().getUserName(),
              obj.getPriorityLevel());
        }
        backOff = false;
      } else {
        // For other RPC calls, backoff or not is based on
        // past response times for queues having higher priorities
        backOff = super.shouldBackOff(obj);
      }
    }
    return backOff;
  }

  @Override
  public void addResponseTime(String name, int priorityLevel, int queueTime,
      int processingTime) {
    if (isReservedLevel(priorityLevel)) {
      // Now we don't process response time for reserved users as we don't enable
      // backoff for reserved users yet.
      // TODO (T1165853): we may revisit this later on.
    } else {
      super.addResponseTime(name, priorityLevel, queueTime, processingTime);
    }
  }

  /**
   * Check whether the given level is in reservation level or not.
   */
  private boolean isReservedLevel(int level) {
    if (level < 0 || level >= numLevels) {
      throw new IllegalArgumentException("The priority level " +
          level + " is invalid. It should be between [0, " + numLevels + ").");
    }
    return level >= this.numSharedLevels;
  }
}
