package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.junit.Assert;
import org.junit.Test;

public class TestScorerEventDispatcherMetrics {

    @Test
    public void testMetrics() {
        ScorerEventDispatcherMetrics metrics = ScorerEventDispatcherMetrics.registerMetrics();
        Assert.assertEquals(0, metrics.containerAddedCount.value());

        Event event = new ScorerContainerEvent(mock(RMContainer.class), ScorerEventType.CONTAINER_ADDED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.containerAddedCount.value());
        Assert.assertEquals(10, metrics.containerAddedTimeUs.value());

        event = new ScorerContainerEvent(mock(RMContainer.class), ScorerEventType.CONTAINER_RECOVERED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.containerRecoveredCount.value());
        Assert.assertEquals(10, metrics.containerRecoveredTimeUs.value());

        event = new ScorerContainerEvent(mock(RMContainer.class), ScorerEventType.AM_CONTAINER_ADDED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.amContainerAddedCount.value());
        Assert.assertEquals(10, metrics.amContainerAddedTimeUs.value());

        event = new ScorerContainerEvent(mock(RMContainer.class), ScorerEventType.CONTAINER_FINISHED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.containerFinishedCount.value());
        Assert.assertEquals(10, metrics.containerFinishedTimeUs.value());

        Set<String> include = new HashSet<>(Arrays.asList("a", "b", "c"));
        event = new ScorerHostEvent(include, ScorerEventType.INCLUDE_HOSTS_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(3, metrics.numberOfIncludeHosts.value());
        Assert.assertEquals(10, metrics.includeHostsTimeUs.value());

        Set<String> exclude = new HashSet<>(Arrays.asList("a", "b"));
        event = new ScorerHostEvent(exclude, ScorerEventType.EXCLUDE_HOSTS_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(2, metrics.numberOfExcludeHosts.value());
        Assert.assertEquals(10, metrics.excludeHostsTimeUs.value());
    }
}
