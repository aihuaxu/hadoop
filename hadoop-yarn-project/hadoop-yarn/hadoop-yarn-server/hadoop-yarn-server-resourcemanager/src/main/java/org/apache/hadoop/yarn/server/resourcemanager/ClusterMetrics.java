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

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class ClusterMetrics {

  @Metric("# of active stressed NMs") MutableGaugeInt numStressedNodes;
  @Metric("# of active NMs") MutableGaugeInt numActiveNMs;
  @Metric("# of decommissioning NMs") MutableGaugeInt numDecommissioningNMs;
  @Metric("# of decommissioned NMs") MutableGaugeInt numDecommissionedNMs;
  @Metric("# of lost NMs") MutableGaugeInt numLostNMs;
  @Metric("# of unhealthy NMs") MutableGaugeInt numUnhealthyNMs;
  @Metric("# of Rebooted NMs") MutableGaugeInt numRebootedNMs;
  @Metric("# of Shutdown NMs") MutableGaugeInt numShutdownNMs;
  @Metric("AM container launch delay") MutableRate aMLaunchDelay;
  @Metric("AM register delay") MutableRate aMRegisterDelay;
  @Metric("AM container allocation delay")
  private MutableRate aMContainerAllocationDelay;

  private static final MetricsInfo RECORD_INFO = info("ClusterMetrics",
  "Metrics for the Yarn Cluster");
  protected static final MetricsInfo CLUSTER_PARTITION_INFO =
          info("ClusterPartition", "Metrics by Partition");
  protected final MetricsRegistry registry;

  // Map to hold per partition metrics
  private static final Map<String, ClusterMetrics> CLUSTER_METRICS_MAP =
          new HashMap<>();
  // Default partition
  private static final String DEFAULT_PARTITION = CommonNodeLabelsManager.NO_LABEL;

  public ClusterMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
  }

  public static ClusterMetrics getMetrics() {
    return getMetrics(DEFAULT_PARTITION);
  }

  protected ClusterMetrics tag(MetricsInfo info, String value) {
    registry.tag(info, value);
    return this;
  }

  protected static StringBuilder sourceName(String partition) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    sb.append(",partition").append('=').append(partition);
    return sb;
  }

  public synchronized static ClusterMetrics getMetrics(String partition) {
    ClusterMetrics metrics = CLUSTER_METRICS_MAP.get(partition);
    if (metrics == null) {
      metrics = new ClusterMetrics().tag(CLUSTER_PARTITION_INFO, partition);
      registerMetrics(partition, metrics);
      CLUSTER_METRICS_MAP.put(partition, metrics);
    }
    return metrics;
  }

  private static void registerMetrics(String partition, ClusterMetrics instance) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      // Keep it backward compatible
      if (partition.equals(DEFAULT_PARTITION)) {
        ms.register("ClusterMetrics", "Cluster Metrics for the Default Partition", instance);
      } else {
        ms.register(sourceName(partition).toString(), "Cluster Metrics for partition:" + partition, instance);
      }
    }
  }

  @VisibleForTesting
  synchronized static void destroy() {
    CLUSTER_METRICS_MAP.clear();
  }
  
  //Active NodeManagers
  public int getNumActiveNMs() {
    return numActiveNMs.value();
  }

  // Decommissioning NMs
  public int getNumDecommissioningNMs() {
    return numDecommissioningNMs.value();
  }

  public void incrDecommissioningNMs() {
    numDecommissioningNMs.incr();
  }

  public void setDecommissioningNMs(int num) {
    numDecommissioningNMs.set(num);
  }

  public void decrDecommissioningNMs() {
    numDecommissioningNMs.decr();
  }

  //Decommisioned NMs
  public int getNumDecommisionedNMs() {
    return numDecommissionedNMs.value();
  }

  public void incrDecommisionedNMs() {
    numDecommissionedNMs.incr();
  }

  public void setDecommisionedNMs(int num) {
    numDecommissionedNMs.set(num);
  }

  public void decrDecommisionedNMs() {
    numDecommissionedNMs.decr();
  }
  
  //Lost NMs
  public int getNumLostNMs() {
    return numLostNMs.value();
  }

  public void incrNumLostNMs() {
    numLostNMs.incr();
  }
  
  public void decrNumLostNMs() {
    numLostNMs.decr();
  }
  
  //Unhealthy NMs
  public int getUnhealthyNMs() {
    return numUnhealthyNMs.value();
  }

  public void incrNumUnhealthyNMs() {
    numUnhealthyNMs.incr();
  }
  
  public void decrNumUnhealthyNMs() {
    numUnhealthyNMs.decr();
  }
  
  //Rebooted NMs
  public int getNumRebootedNMs() {
    return numRebootedNMs.value();
  }
  
  public void incrNumRebootedNMs() {
    numRebootedNMs.incr();
  }
  
  public void decrNumRebootedNMs() {
    numRebootedNMs.decr();
  }

  // Shutdown NMs
  public int getNumShutdownNMs() {
    return numShutdownNMs.value();
  }

  public void incrNumShutdownNMs() {
    numShutdownNMs.incr();
  }

  public void decrNumShutdownNMs() {
    numShutdownNMs.decr();
  }

  public void incrNumActiveNodes() {
    numActiveNMs.incr();
  }

  public void decrNumActiveNodes() {
    numActiveNMs.decr();
  }

  public void addAMLaunchDelay(long delay) {
    aMLaunchDelay.add(delay);
  }

  public void addAMRegisterDelay(long delay) {
    aMRegisterDelay.add(delay);
  }

  public void addAMContainerAllocationDelay(long delay) {
    aMContainerAllocationDelay.add(delay);
  }

  public MutableRate getAMContainerAllocationDelay() {
    return aMContainerAllocationDelay;
  }

  public void incrStressedNodes() {
    numStressedNodes.incr();
  }

  public void decrStressedNodes() {
    numStressedNodes.decr();
  }

  public int getNumStressedNodes() {
    return numStressedNodes.value();
  }
}
