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
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;

import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "capacityScheduler")
@XmlType(name = "capacityScheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerInfo extends SchedulerInfo {

  protected float capacity;
  protected float usedCapacity;
  protected float maxCapacity;
  protected String queueName;
  protected CapacitySchedulerQueueInfoList queues;
  protected QueueCapacitiesInfo capacities;
  protected CapacitySchedulerHealthInfo health;
  @XmlTransient
  protected CapacityScheduler cs;

  @XmlTransient
  static final float EPSILON = 1e-8f;

  public CapacitySchedulerInfo() {
  } // JAXB needs this

  public CapacitySchedulerInfo(CSQueue queue, CapacityScheduler cs) {
    this.queueName = queue.getQueueName();
    this.usedCapacity = queue.getUsedCapacity() * 100;
    this.capacity = queue.getCapacity() * 100;
    float max = queue.getMaximumCapacity();
    if (max < EPSILON || max > 1f)
      max = 1f;
    this.maxCapacity = max * 100;

    this.cs = cs;
    capacities = new QueueCapacitiesInfo(cs, queue.getQueueCapacities(), false);
    queues = getQueues(queue);
    health = new CapacitySchedulerHealthInfo(cs);
  }

  public float getCapacity() {
    return this.capacity;
  }

  public float getUsedCapacity() {
    return this.usedCapacity;
  }

  public QueueCapacitiesInfo getCapacities() {
    return capacities;
  }

  public float getMaxCapacity() {
    return this.maxCapacity;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public CapacitySchedulerQueueInfoList getQueues() {
    return this.queues;
  }

  protected CapacitySchedulerQueueInfoList getQueues(CSQueue queue) {
    CapacitySchedulerQueueInfoList queuesInfo =
        new CapacitySchedulerQueueInfoList();

    // Include the current leaf queue
    if (queue instanceof LeafQueue) {
      queuesInfo.addToQueueInfoList(
              new CapacitySchedulerLeafQueueInfo(cs, (LeafQueue) queue));
      return queuesInfo;
    }

    // JAXB marashalling leads to situation where the "type" field injected
    // for JSON changes from string to array depending on order of printing
    // Issue gets fixed if all the leaf queues are marshalled before the
    // non-leaf queues. See YARN-4785 for more details.
    List<CSQueue> childQueues = new ArrayList<>();
    List<CSQueue> childLeafQueues = new ArrayList<>();
    List<CSQueue> childNonLeafQueues = new ArrayList<>();

    List<CSQueue> queues = queue.getChildQueues();
    if (queues != null) {
      for (CSQueue childQueue : queues) {
        if (childQueue instanceof LeafQueue) {
          childLeafQueues.add(childQueue);
        } else {
          childNonLeafQueues.add(childQueue);
        }
      }
    }
    childQueues.addAll(childLeafQueues);
    childQueues.addAll(childNonLeafQueues);

    for (CSQueue childQueue : childQueues) {
      CapacitySchedulerQueueInfo info;
      if (childQueue instanceof LeafQueue) {
        info = new CapacitySchedulerLeafQueueInfo(cs, (LeafQueue) childQueue);
      } else {
        info = new CapacitySchedulerQueueInfo(cs, childQueue);
        info.queues = getQueues(childQueue);
      }
      queuesInfo.addToQueueInfoList(info);
    }
    return queuesInfo;
  }
}
