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

package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.yarn.event.DispatcherMetrics;
import org.apache.hadoop.yarn.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@InterfaceAudience.Private
@Metrics(context="yarn")
public class ScorerEventDispatcherMetrics extends DispatcherMetrics {

  static final Logger LOG = LoggerFactory.getLogger(ScorerEventDispatcherMetrics.class);
  static final MetricsInfo RECORD_INFO = info("ScorerEventDispatcherMetrics",
      "Metrics for scorer async dispatcher");

  static boolean initialized;
  static ScorerEventDispatcherMetrics metrics = null;

  @Metric("Number of containers") MutableGaugeLong containers;
  @Metric("Number of AM containers") MutableGaugeLong amContainers;
  @Metric("Number of non-preemptible containers") MutableGaugeLong nonPreemptibleContainers;

  @Metric("Number of include hosts") MutableGaugeLong numberOfIncludeHosts;
  @Metric("Include hosts event processing time") MutableCounterLong includeHostsTimeUs;
  @Metric("Number of exclude hosts") MutableCounterLong numberOfExcludeHosts;
  @Metric("Exclude hosts event processing time") MutableCounterLong excludeHostsTimeUs;

  @Metric("getOrderedHostsList processing time") MutableCounterLong getOrderedHostsListTimeUs;
  @Metric("updateRunningContainerTask processing time") MutableCounterLong updateHostScoreTimeUs;

  @Metric("Host score update succeeded") MutableGaugeLong updateHostScoreSucceeded;

  protected ScorerEventDispatcherMetrics(MetricsSystem ms) {
    super(ms, RECORD_INFO);
  }

  protected static StringBuilder sourceName() {
    return new StringBuilder(RECORD_INFO.name());
  }

  public synchronized
  static ScorerEventDispatcherMetrics registerMetrics() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (!initialized) {
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = new ScorerEventDispatcherMetrics(ms);
        LOG.info("Registering ScorerEventDispatcherMetrics");
        ms.register(
            sourceName().toString(),
            "Metrics for scorer async dispatcher", metrics);
        initialized = true;
      } else {
        LOG.warn("Failed to initialize ScorerEventDispatcherMetrics because DefaultMetricsSystem is null ");
      }
    }

    return metrics;
  }

  public void incrementEventType(Event event, long processingTimeUs) {
    LOG.debug("Got scorer event of type " + event.getType());
    switch ((ScorerEventType) (event.getType())) {
      case INCLUDE_HOSTS_UPDATE:
        Set<String> includeHosts = ((ScorerHostEvent) event).getHosts();
        numberOfIncludeHosts.set(includeHosts == null ? 0 : includeHosts.size());
        includeHostsTimeUs.incr(processingTimeUs);
        break;
      case EXCLUDE_HOSTS_UPDATE:
        Set<String> excludeHosts = ((ScorerHostEvent) event).getHosts();
        numberOfExcludeHosts.incr(excludeHosts == null ? 0 : excludeHosts.size());
        excludeHostsTimeUs.incr(processingTimeUs);
        break;
      default:
        break;
    }
  }

  public void setContainers(long numberOfContainers) {
    containers.set(numberOfContainers);
  }

  public void setAMContainers(long numberOfAMContainers) {
    amContainers.set(numberOfAMContainers);
  }

  public void setNonPreemptibleContainers(long numberOfNonPreemptibleContainers) {
    nonPreemptibleContainers.set(numberOfNonPreemptibleContainers);
  }

  public void incrGetOrderedHostsListTimeUs(long processTimeUs) {
    getOrderedHostsListTimeUs.incr(processTimeUs);
  }

  public void incrUpdateHostScoreTimeUs(long processTimeUs) {
    updateHostScoreTimeUs.incr(processTimeUs);
  }

  public void setUpdateHostScoreSucceeded(long updateHostScoreSucceeded) {
    this.updateHostScoreSucceeded.set(updateHostScoreSucceeded);
  }

}
