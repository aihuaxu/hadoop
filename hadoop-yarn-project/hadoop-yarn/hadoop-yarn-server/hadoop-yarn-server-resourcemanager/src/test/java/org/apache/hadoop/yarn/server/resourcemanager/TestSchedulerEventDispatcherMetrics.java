package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.junit.Assert;
import org.junit.Test;

public class TestSchedulerEventDispatcherMetrics {

    @Test
    public void testMetrics() {
        SchedulerEventDispatcherMetrics metrics = SchedulerEventDispatcherMetrics.registerMetrics();
        Assert.assertEquals(0, metrics.nodeAddedCount.value());

        Event event = new SchedulerEvent(SchedulerEventType.NODE_ADDED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.nodeAddedCount.value());
        Assert.assertEquals(10, metrics.nodeAddedTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.NODE_REMOVED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.nodeRemovedCount.value());
        Assert.assertEquals(10, metrics.nodeRemovedTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.NODE_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.nodeUpdateCount.value());
        Assert.assertEquals(10, metrics.nodeUpdateTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.NODE_RESOURCE_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.nodeResourceUpdateCount.value());
        Assert.assertEquals(10, metrics.nodeResourceUpdateTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.NODE_LABELS_UPDATE);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.nodeLabelsUpdateCount.value());
        Assert.assertEquals(10, metrics.nodeLabelsUpdateTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.APP_ADDED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.appAddedCount.value());
        Assert.assertEquals(10, metrics.appAddedTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.APP_REMOVED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.appRemovedCount.value());
        Assert.assertEquals(10, metrics.appRemovedTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.APP_ATTEMPT_ADDED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.appAttemptAddedCount.value());
        Assert.assertEquals(10, metrics.appAttemptAddedTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.APP_ATTEMPT_REMOVED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.appAttemptRemovedCount.value());
        Assert.assertEquals(10, metrics.appAttemptRemovedTimeUs.value());

        event = new SchedulerEvent(SchedulerEventType.CONTAINER_EXPIRED);
        metrics.incrementEventType(event, 10);
        Assert.assertEquals(1, metrics.containerExpiredCount.value());
        Assert.assertEquals(10, metrics.containerExpiredTimeUs.value());
    }
}
