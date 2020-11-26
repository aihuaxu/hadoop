package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.yarn.event.Event;
import org.junit.Assert;
import org.junit.Test;

public class TestScorerEventDispatcherMetrics {

    @Test
    public void testMetrics() {
        ScorerEventDispatcherMetrics metrics = ScorerEventDispatcherMetrics.registerMetrics();

        Set<String> include = new HashSet<>(Arrays.asList("a", "b", "c"));
        Event event = new ScorerHostEvent(include, ScorerEventType.INCLUDE_HOSTS_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(3, metrics.numberOfIncludeHosts.value());
        Assert.assertEquals(10, metrics.includeHostsTimeUs.value());

        Set<String> exclude = new HashSet<>(Arrays.asList("a", "b"));
        event = new ScorerHostEvent(exclude, ScorerEventType.EXCLUDE_HOSTS_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(2, metrics.numberOfExcludeHosts.value());
        Assert.assertEquals(10, metrics.excludeHostsTimeUs.value());


        metrics.incrGetOrderedHostsListTimeUs(8);
        Assert.assertEquals(8, metrics.getOrderedHostsListTimeUs.value());

        metrics.incrUpdateHostScoreTimeUs(9);
        Assert.assertEquals(9, metrics.updateHostScoreTimeUs.value());

        metrics.setUpdateHostScoreSucceeded(1);
        Assert.assertEquals(1, metrics.updateHostScoreSucceeded.value());

        metrics.setContainers(5);
        Assert.assertEquals(5, metrics.containers.value());

        metrics.setAMContainers(2);
        Assert.assertEquals(2, metrics.amContainers.value());

        metrics.setNonPreemptibleContainers(4);
        Assert.assertEquals(4, metrics.nonPreemptibleContainers.value());
    }
}
