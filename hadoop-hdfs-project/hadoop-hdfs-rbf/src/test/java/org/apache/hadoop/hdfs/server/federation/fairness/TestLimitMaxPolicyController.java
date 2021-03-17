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

import static org.apache.hadoop.hdfs.server.federation.router.Constants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

/**
 * Test functionality of {@link LimitMaxPolicyController}.
 */
public class TestLimitMaxPolicyController {

    @Test
    public void testAssignHandlersToNameservicesDefault() throws IOException {
        // Expected handler assignment should be:
        //   total handlers: 10
        //   ns1: 5
        //   ns2: 5
        //   concurrent: 5
        Configuration conf = getDefaultConf(10);
        LimitMaxPolicyController c = new LimitMaxPolicyController(conf);

        // Verify permists for global settings.
        assertEquals(10, c.getGlobalPermits().availablePermits());
        assertEquals(100, c.getPermitAcquireTimeout());

        // Verify permists for each name service.
        Map<String, Semaphore> nsPermits = c.getNsPermits();
        assertEquals(3, nsPermits.size());

        Set<String> expectedKeySet = new HashSet<>();
        expectedKeySet.add("ns1");
        expectedKeySet.add("ns2");
        expectedKeySet.add("concurrent");
        assertTrue(CollectionUtils.isEqualCollection(expectedKeySet, nsPermits.keySet()));

        for (Semaphore nsPermit: nsPermits.values()) {
            assertEquals(5, nsPermit.availablePermits());
        }
    }

    @Test
    public void testAssignHandlersToNameservicesNsOverride() throws IOException {
        Configuration conf = getDefaultConf(10);
        // Override default conf. Expected handler assignment should be:
        //   total handlers: 10
        //   ns1: 3
        //   ns2: 6
        //   concurrent: 2
        conf.set(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_GLOBAL_KEY, "0.6f");
        conf.set(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_KEY_PREFIX + "ns1", "0.3f");
        conf.set(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_KEY_PREFIX + CONCURRENT_NS, "0.2f");
        conf.set(DFS_ROUTER_FAIR_HANDLER_ACQUIRE_TIMEOUT_KEY, "50");

        LimitMaxPolicyController c = new LimitMaxPolicyController(conf);

        // Verify permists for global settings.
        assertEquals(10, c.getGlobalPermits().availablePermits());
        assertEquals(50, c.getPermitAcquireTimeout());

        // Verify permists for each name service.
        Map<String, Semaphore> nsPermits = c.getNsPermits();
        assertEquals(3, nsPermits.size());
        assertEquals(3, nsPermits.get("ns1").availablePermits());
        assertEquals(6, nsPermits.get("ns2").availablePermits());
        assertEquals(2, nsPermits.get(CONCURRENT_NS).availablePermits());
    }

    @Test
    public void testAssignHandlersToNameservicesNsTotalOne() throws IOException {
        Configuration conf = getDefaultConf(1);
        // Override default conf. Expected handler assignment should be:
        //   total handlers: 1
        //   ns1: 0 (ns1 should be 0 as it sets 0 explicitly)
        //   ns2: 1
        //   concurrent: 1
        conf.set(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_GLOBAL_KEY, "0.6f");
        conf.set(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_KEY_PREFIX + "ns1", "0");
        conf.set(DFS_ROUTER_FAIR_HANDLER_MAX_RATIO_KEY_PREFIX + CONCURRENT_NS, "0.1f");

        LimitMaxPolicyController c = new LimitMaxPolicyController(conf);

        // Verify permists for global settings.
        assertEquals(1, c.getGlobalPermits().availablePermits());
        assertEquals(100, c.getPermitAcquireTimeout());

        // Verify permists for each name service.
        Map<String, Semaphore> nsPermits = c.getNsPermits();
        assertEquals(3, nsPermits.size());
        assertEquals(0, nsPermits.get("ns1").availablePermits());
        assertEquals(1, nsPermits.get("ns2").availablePermits());
        assertEquals(1, nsPermits.get(CONCURRENT_NS).availablePermits());
    }

    @Test (expected = PermitAllocationException.class)
    public void testAssignHandlersToNameservicesInvalidNs() throws IOException {
        Configuration conf = getDefaultConf(10);
        conf.set(DFS_ROUTER_MONITOR_NAMENODE, "bad.ns.string, ns1.nn2, ns1.onn1, ns1.onn2, ns2.nn1, ns2.nn2");
        new LimitMaxPolicyController(conf);
    }

    @Test
    public void testAcquireReleasePermit() throws IOException {
        LimitMaxPolicyController c = new LimitMaxPolicyController(getDefaultConf(10));
        Semaphore ns1Permit = c.getNsPermits().get("ns1");
        // Normal flow.
        for (int i = 0; i < 5; i++) {
            c.acquirePermit("ns1");
        }
        assertEquals(0, ns1Permit.availablePermits());
        assertEquals(5, c.getGlobalPermits().availablePermits());
        c.releasePermit("ns1");
        assertEquals(1, ns1Permit.availablePermits());
        assertEquals(6, c.getGlobalPermits().availablePermits());

        c.acquirePermit("ns1");
        // The sixth acquire request should fail.
        try {
            c.acquirePermit("ns1");
        } catch (Exception e) {
            assertTrue(e instanceof NoPermitAvailableException);
        }
        assertEquals(0, ns1Permit.availablePermits());
        assertEquals(5, c.getGlobalPermits().availablePermits());

        // Request to ns2 should not be blocked.
        c.acquirePermit("ns2");
        assertEquals(4, c.getNsPermits().get("ns2").availablePermits());
        assertEquals(4, c.getGlobalPermits().availablePermits());

        // Request to concurrent_ns should not be blocked.
        c.acquirePermit(CONCURRENT_NS);
        assertEquals(4, c.getNsPermits().get(CONCURRENT_NS).availablePermits());
        assertEquals(3, c.getGlobalPermits().availablePermits());

    }

    @Test
    public void testShutdown() throws IOException {
        LimitMaxPolicyController c = new LimitMaxPolicyController(getDefaultConf(10));
        c.shutdown();
    }

    private Configuration getDefaultConf(int handler) {
        Configuration conf = new Configuration();
        conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handler);
        conf.set(DFS_ROUTER_MONITOR_NAMENODE, "ns1.nn1, ns1.nn2, ns1.onn1, ns1.onn2, ns2.nn1, ns2.nn2");
        return conf;
    }
}
