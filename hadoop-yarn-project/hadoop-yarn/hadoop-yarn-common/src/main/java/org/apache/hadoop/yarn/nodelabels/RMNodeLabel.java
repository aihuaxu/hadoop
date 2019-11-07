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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

@Metrics(context="yarn")
public class RMNodeLabel implements Comparable<RMNodeLabel> {
  private Resource resource;
  @Metric("# of active NMs") MutableGaugeInt numActiveNMs;
  private String labelName;
  private Set<NodeId> nodeIds;
  private boolean exclusive;
  private NodeLabel nodeLabel;
  private MetricsRegistry registry;
  private final MetricsInfo RECORD_INFO = info("NodeLabelsMetrics",
    "Metrics for the Yarn Cluster Node Labels");


  public RMNodeLabel(NodeLabel nodeLabel) {
    this(nodeLabel.getName(), Resource.newInstance(0, 0), 0,
        nodeLabel.isExclusive());
  }

  public RMNodeLabel(String labelName) {
    this(labelName, Resource.newInstance(0, 0), 0,
        NodeLabel.DEFAULT_NODE_LABEL_EXCLUSIVITY);
  }
  
  protected RMNodeLabel(String labelName, Resource res, int activeNMs,
      boolean exclusive) {
    this.labelName = labelName;
    this.resource = res;
    this.nodeIds = new HashSet<NodeId>();
    this.exclusive = exclusive;
    this.nodeLabel = NodeLabel.newInstance(labelName, exclusive);

    registerMetrics();
    this.numActiveNMs.set(activeNMs);
  }

  private void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      String partitionName = labelName == null || labelName.equals("") ? "default" : labelName;
      ms.register("NodeLabelsMetrics,label=" + partitionName,
          "Metrics for the Yarn Cluster Node Labels, label: " + partitionName, this);
    }
  }

  public void addNodeId(NodeId node) {
    nodeIds.add(node);
  }

  public void removeNodeId(NodeId node) {
    nodeIds.remove(node);
  }
  
  public Set<NodeId> getAssociatedNodeIds() {
    return new HashSet<NodeId>(nodeIds);
  }

  public void addNode(Resource nodeRes) {
    Resources.addTo(resource, nodeRes);
    numActiveNMs.incr();
  }
  
  public void removeNode(Resource nodeRes) {
    Resources.subtractFrom(resource, nodeRes);
    numActiveNMs.decr();
  }

  public Resource getResource() {
    return this.resource;
  }

  public int getNumActiveNMs() {
    return numActiveNMs.value();
  }
  
  public String getLabelName() {
    return labelName;
  }
  
  public void setIsExclusive(boolean exclusive) {
    this.exclusive = exclusive;
  }
  
  public boolean getIsExclusive() {
    return this.exclusive;
  }

  public NodeLabel getNodeLabel() {
    return this.nodeLabel;
  }

  @Override
  public int compareTo(RMNodeLabel o) {
    // We should always put empty label entry first after sorting
    if (labelName.isEmpty() != o.getLabelName().isEmpty()) {
      if (labelName.isEmpty()) {
        return -1;
      }
      return 1;
    }
    
    return labelName.compareTo(o.getLabelName());
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RMNodeLabel) {
      RMNodeLabel other = (RMNodeLabel) obj;
      return Resources.equals(resource, other.getResource())
          && StringUtils.equals(labelName, other.getLabelName())
          && (other.getNumActiveNMs() == numActiveNMs.value());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    final int prime = 502357;
    return (int) ((((long) labelName.hashCode() << 8)
        + (resource.hashCode() << 4) + numActiveNMs.value()) % prime);
  }
}
