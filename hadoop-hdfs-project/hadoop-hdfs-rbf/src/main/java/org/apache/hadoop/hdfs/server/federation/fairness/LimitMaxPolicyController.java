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
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.server.federation.router.Constants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;

/**
 * Limit max implements @FairnessPolicyController and set an upper limit for all available name services.
 * The upper limit is a percentage of total handlers. A global ratio is provided and each name service is
 * capable of overriding.
 */
public class LimitMaxPolicyController implements FairnessPolicyController {

    private static final Logger LOG =
            LoggerFactory.getLogger(LimitMaxPolicyController.class);

    // Global semaphore to hold total handlers.
    private Semaphore globalPermits;

    // Hash table to hold semaphore for each configured name service.
    private Map<String, Semaphore> nsPermits;

    // Timeout for acquiring permit from globalPermits and nsPermits.
    private long permitAcquireTimeout;

    // PermitType indicated if a permit is at global level or per cluster level.
    enum PermitType {
        // global semaphore.
        GLOBAL,
        // local semaphore for each individual name service.
        LOCAL
    }

    public LimitMaxPolicyController(Configuration conf) throws IOException {
        this.permitAcquireTimeout = conf.getLong(DFS_ROUTER_FAIR_HANDLER_ACQUIRE_TIMEOUT_KEY,
                DFS_ROUTER_FAIR_HANDLER_ACQUIRE_TIMEOUT_DEFAULT);
        assignHandlersToNameservices(conf);
    }

    @Override
    public void assignHandlersToNameservices(Configuration conf)
            throws PermitAllocationException {
        this.nsPermits = new HashMap();

        // Total handlers configured to process all incoming Rpc.
        int handlerCount = conf.getInt(
                DFS_ROUTER_HANDLER_COUNT_KEY,
                DFS_ROUTER_HANDLER_COUNT_DEFAULT);
        LOG.info("Handlers available for fairness assignment {} ", handlerCount);

        globalPermits = new Semaphore(handlerCount);
        float globalMaxRatio = conf.getFloat(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_GLOBAL_KEY,
                DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_GLOBAL_DEFAULT);

        // Get all name services configured
        Collection<String> namenodes = conf.getTrimmedStringCollection(
                DFS_ROUTER_MONITOR_NAMENODE);

        for (String namenode : namenodes) {
            String nsId = Utils.getNsId(namenode);
            if (nsId.isEmpty()) {
                String errorMsg = "Wrong name service specified :" + namenode;
                LOG.error(errorMsg);
                throw new PermitAllocationException(errorMsg);
            }
            if (!this.nsPermits.containsKey(nsId)) {
                int dedicatedMaxHandlers = calculateHandlers(conf, nsId, handlerCount, globalMaxRatio);
                this.nsPermits.put(nsId, new Semaphore(dedicatedMaxHandlers));
                LOG.info("Set max handlers {} for ns {} ", dedicatedMaxHandlers, nsId);
            }
        }

        // Assign dedicated handlers for fan-out calls if configured.
        int dedicatedMaxHandlers = calculateHandlers(conf, CONCURRENT_NS, handlerCount, globalMaxRatio);
        this.nsPermits.put(
                CONCURRENT_NS, new Semaphore(dedicatedMaxHandlers));
        LOG.info("Set max handlers {} for ns {} ", dedicatedMaxHandlers, CONCURRENT_NS);
    }

    /**
     * Calculate the expected max handlers per NS based on configuration.
     *
     * @param conf router configuration.
     * @param nsId name service to assign handlers.
     * @param handlerCount total available handlers.
     * @param globalMaxRatio a global default ratio for calculating max handlers.
     * @return the max handlers for given nsId.
     */
    private int calculateHandlers(Configuration conf, String nsId, int handlerCount, float globalMaxRatio) {
        float dedicatedMaxRatio = conf.getFloat(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_KEY_PREFIX + nsId,
                globalMaxRatio);
        if (dedicatedMaxRatio == 0) return 0;
        if (handlerCount == 1) return 1;
        return (int) ( dedicatedMaxRatio * handlerCount );
    }

    @Override
    public void acquirePermit(String nsId) throws NoPermitAvailableException {
        try {
            // Acquire per-ns permit first to avoid exhausting global permit
            // by high volume requests to single ns.
            if (!this.nsPermits.get(nsId).tryAcquire(this.permitAcquireTimeout, TimeUnit.MILLISECONDS)) {
                throw new NoPermitAvailableException(nsId, PermitType.LOCAL.name());
            }
            try {
                if (!this.globalPermits.tryAcquire(this.permitAcquireTimeout, TimeUnit.MILLISECONDS)) {
                    this.nsPermits.get(nsId).release();
                    throw new NoPermitAvailableException(nsId, PermitType.GLOBAL.name());
                }
            } catch (NoPermitAvailableException e) {
                throw e;
            } catch (Exception e) {
                this.nsPermits.get(nsId).release();
                String message = String.format("Encountered exception while getting permit, " +
                        "ns %s, type %s", nsId, PermitType.GLOBAL.name());
                LOG.error(message, e);
                throw new NoPermitAvailableException(nsId, PermitType.GLOBAL.name());
            }
        } catch (InterruptedException ie) {
            String message = String.format("Encountered InterruptedException while waiting for permit, " +
                    "ns %s, type %s", nsId, PermitType.LOCAL.name());
            LOG.error(message, ie);
            throw new NoPermitAvailableException(nsId, PermitType.LOCAL.name());
        }
    }

    @Override
    public void releasePermit(String nsId) {
        this.globalPermits.release();
        this.nsPermits.get(nsId).release();
    }

    @Override
    public void shutdown() {
        // Nothing for now.
    }

    /**
     * Get semaphore which holds total available permits.
     * This function is intended for unit test purpose.
     *
     * @return the semaphore with total available permits.
     */
    public Semaphore getGlobalPermits() {
        return globalPermits;
    }

    /**
     * Get a map of semaphore for each name services.
     * This function is intended for unit test purpose.
     *
     * @return a map of nsId -> semaphore.
     */
    public Map<String, Semaphore> getNsPermits() {
        return nsPermits;
    }

    /**
     * Get the timeout for acquiring permits.
     * This function is intended for unit test purpose.
     *
     * @return the timeout in millisecond for acquiring permits.
     */
    public long getPermitAcquireTimeout() {
        return permitAcquireTimeout;
    }
}
