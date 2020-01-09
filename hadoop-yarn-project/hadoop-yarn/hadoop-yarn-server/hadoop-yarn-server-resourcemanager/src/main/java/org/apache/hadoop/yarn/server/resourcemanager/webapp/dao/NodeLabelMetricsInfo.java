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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;

@XmlRootElement(name = "nodeLabelMetricsInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeLabelMetricsInfo {

  private String name;
  private long numActiveNMs;
  private ResourceInfo resourceInfo;

  public NodeLabelMetricsInfo() {
    // JAXB needs this
  }

  public NodeLabelMetricsInfo(String name, ResourceInfo resourceInfo) {
    this.name = name;
    this.resourceInfo = resourceInfo;
  }

  public NodeLabelMetricsInfo(String name, Resource resource) {
    this.name = name;
    this.resourceInfo = new ResourceInfo(resource);
  }

  public NodeLabelMetricsInfo(RMNodeLabel rmNodeLabel) {
    this.name = rmNodeLabel.getLabelName();
    this.numActiveNMs = rmNodeLabel.getNumActiveNMs();
    this.resourceInfo = new ResourceInfo(rmNodeLabel.getResource());
  }

  public String getName() {
    return name;
  }

  public ResourceInfo getResourceInfo() {
    return resourceInfo;
  }

  public long getNumActiveNMs() { return numActiveNMs; }
}
