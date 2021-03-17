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

package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collection;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hdfs.server.federation.router.Constants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;

/**
 * Default fairness policy that implements @FairnessPolicyController and assigns
 * equal or configured handlers to all available name services.
 */
public class DefaultFairnessPolicyController implements
        FairnessPolicyController {

  private static final Logger LOG =
          LoggerFactory.getLogger(DefaultFairnessPolicyController.class);

  /** Hash table to hold semaphore for each configured name service. */
  private Map<String, Semaphore> permits;

  public DefaultFairnessPolicyController(Configuration conf)
          throws IOException {
    assignHandlersToNameservices(conf);
  }

  @Override
  public void assignHandlersToNameservices(Configuration conf)
          throws PermitAllocationException {
    this.permits = new HashMap();

    // Total handlers configured to process all incoming Rpc.
    int handlerCount = conf.getInt(
            DFS_ROUTER_HANDLER_COUNT_KEY,
            DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    LOG.info("Handlers available for fairness assignment {} ", handlerCount);

    // Get all name services configured
    Collection<String> namenodes = conf.getTrimmedStringCollection(
            DFS_ROUTER_MONITOR_NAMENODE);

    // Set to hold name services that are not
    // configured with dedicated handlers.
    Set<String> unassignedNS = new HashSet();

    for (String namenode : namenodes) {
      String nsId = Utils.getNsId(namenode);
      if (nsId.isEmpty()) {
        String errorMsg = "Wrong name service specified :" + namenode;
        LOG.error(errorMsg);
        throw new PermitAllocationException(errorMsg);
      }
      int dedicatedHandlers =
              conf.getInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId, 0);
      LOG.info("Dedicated handlers {} for ns {} ", dedicatedHandlers, nsId);
      if (dedicatedHandlers > 0) {
        if (!this.permits.containsKey(nsId)) {
          handlerCount -= dedicatedHandlers;
          // Total handlers should not be less than sum of dedicated
          // handlers.
          validateCount(handlerCount, 0);
          this.permits.put(nsId, new Semaphore(dedicatedHandlers));
          logAssignment(nsId, dedicatedHandlers);
        }
      } else {
        unassignedNS.add(nsId);
      }
    }

    // Assign dedicated handlers for fan-out calls if configured.
    int dedicatedHandlersConcurrent = conf.getInt(
            DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + CONCURRENT_NS, 0);
    if (dedicatedHandlersConcurrent > 0) {
      handlerCount -= dedicatedHandlersConcurrent;
      validateCount(handlerCount, 0);
      this.permits.put(
              CONCURRENT_NS, new Semaphore(dedicatedHandlersConcurrent));
      logAssignment(CONCURRENT_NS, dedicatedHandlersConcurrent);
    } else {
      unassignedNS.add(CONCURRENT_NS);
    }

    // Assign remaining handlers equally to remaining name services and
    // general pool if applicable.
    if (!unassignedNS.isEmpty()) {
      LOG.info("Unassigned ns {}", unassignedNS.toString());
      int handlersPerNS = handlerCount / unassignedNS.size();
      LOG.info("Handlers available per ns {}", handlersPerNS);
      // Each NS should have at least one handler assigned.
      validateCount(handlersPerNS, 1);
      for (String nsId : unassignedNS) {
        this.permits.put(nsId, new Semaphore(handlersPerNS));
        logAssignment(nsId, handlersPerNS);
      }
    }

    // Assign remaining handlers if any to fan out calls.
    int leftOverHandlers = handlerCount % unassignedNS.size();
    if (leftOverHandlers > 0) {
      LOG.info("Assigned extra {} handlers to commons pool", leftOverHandlers);
      this.permits.get(CONCURRENT_NS).release(leftOverHandlers);
    }
    logFinalAllocation();
  }

  @Override
  public void acquirePermit(String nsId) throws NoPermitAvailableException {
    if (!this.permits.get(nsId).tryAcquire()) {
      throw new NoPermitAvailableException(nsId);
    }
  }

  @Override
  public void releasePermit(String nsId) {
    this.permits.get(nsId).release();
  }

  @Override
  public void shutdown() {
    // Nothing for now
  }

  private void logAssignment(String nsId, int count) {
    LOG.info("Assigned {} handlers to nsId {} ",
            count, nsId);
  }

  private void validateCount(int handlers, int min) throws
          PermitAllocationException {
    if (handlers < min) {
      String msg = "Available handlers "
              + handlers + " lower than min " + min;
      LOG.error(msg);
      throw new PermitAllocationException(msg);
    }
  }

  private void logFinalAllocation() {
    // Logged once during start-up, keep it at "info" level.
    LOG.info("Final permit allocation table {}", this.permits.toString());
  }
}
