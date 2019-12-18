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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.junit.Assert;
import org.junit.Test;

public class TestRMAsyncDispatcherMetrics {

    @Test
    public void testMetrics() {
        RMAsyncDispatcherMetrics metrics = RMAsyncDispatcherMetrics.registerMetrics();
        Assert.assertEquals(0, metrics.nodeUnusableCount.value());

        Event eventUnusable = new NodesListManagerEvent(NodesListManagerEventType.NODE_UNUSABLE, MockNodes.newNodeInfo(1, Resource.newInstance(1,1)));
        Event eventUsable = new NodesListManagerEvent(NodesListManagerEventType.NODE_USABLE, MockNodes.newNodeInfo(1, Resource.newInstance(1,1)));
        metrics.incrementEventType(eventUnusable, 10);
        metrics.incrementEventType(eventUsable, 10);
        Assert.assertEquals(1, metrics.nodeUnusableCount.value());
        Assert.assertEquals(1, metrics.nodeUsableCount.value());
        Assert.assertEquals(10, metrics.nodeUsableTimeUs.value());
        Assert.assertEquals(10, metrics.nodeUnusableCount.value());

    }
}
