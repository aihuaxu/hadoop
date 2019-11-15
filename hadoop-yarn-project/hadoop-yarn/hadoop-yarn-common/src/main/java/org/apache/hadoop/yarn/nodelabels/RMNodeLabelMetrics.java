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

package org.apache.hadoop.yarn.nodelabels;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

@Metrics(context="yarn")
public class RMNodeLabelMetrics {
  private final static MetricsInfo RECORD_INFO = info("NodeLabel",
    "Metrics for the Yarn Cluster Node Label");
  private static final Map<String, RMNodeLabelMetrics> RMNODELABEL_METRICS =
    new HashMap<String, RMNodeLabelMetrics>();

  @Metric("# of active NMs") MutableGaugeInt numActiveNMs;
  private String labelName;
  MetricsRegistry registry;

  public synchronized
  static RMNodeLabelMetrics getNodeLabelMetrics(String labelName, int numActiveNMs) {
    RMNodeLabelMetrics metrics = RMNODELABEL_METRICS.get(labelName);
    if (metrics == null) {
      metrics = new RMNodeLabelMetrics(labelName);
      RMNODELABEL_METRICS.put(labelName, metrics);
    }
    metrics.numActiveNMs.set(numActiveNMs);
    return metrics;
  }

  public synchronized
  static void removeNodeLabelMetrics(String labelName) {
    RMNodeLabelMetrics metrics = RMNODELABEL_METRICS.remove(labelName);
    if (metrics != null) {
      metrics.unRegisterMetrics();
    }
  }

  private RMNodeLabelMetrics(String labelName) {
    this.labelName = labelName;
    registerMetrics();
  }

  protected String getMetricsSourceName() {
    return "NodeLabelsMetrics,label=" + labelName;
  }

  private void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, labelName);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register(getMetricsSourceName(),
        "Metrics for the Yarn Cluster Node Labels, label: " + labelName, this);
    }
  }

  private void unRegisterMetrics() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.unregisterSource(getMetricsSourceName());
    }
  }

  public void incrNumActiveNMs() {
    numActiveNMs.incr();
  }

  public void decrNumActiveNMs() {
    numActiveNMs.decr();
  }

  public int getNumActiveNMs() {
    return numActiveNMs.value();
  }
}
