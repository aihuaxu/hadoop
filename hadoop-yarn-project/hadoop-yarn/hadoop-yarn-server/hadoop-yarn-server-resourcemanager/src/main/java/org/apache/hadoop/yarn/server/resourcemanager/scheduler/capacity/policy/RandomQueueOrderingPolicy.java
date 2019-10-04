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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;


/**
 * Policy which picks a child queue to assign to with equal probability.
 */
public class RandomQueueOrderingPolicy implements QueueOrderingPolicy {

  private List<CSQueue> queues;

  @Override
  public void setQueues(List<CSQueue> queues) {
    this.queues = queues;
  }

  @Override
  public Iterator<CSQueue> getAssignmentIterator(String partition) {
    return new RandomIterator(new ArrayList<>(queues));
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration.RANDOM_ORDERING_POLICY;
  }

  static class RandomIterator<CSQueue> implements Iterator<CSQueue> {
    private int idx;
    private ArrayList<CSQueue> q;
    private Random random;

    // TODO: this is single threaded only
    RandomIterator(ArrayList<CSQueue> q) {
      this.idx = q.size() - 1;
      this.q = q;
      this.random = new Random();
    }

    @Override
    public boolean hasNext() {
      return idx >= 0;
    }

    @Override
    public CSQueue next() {
      int selected = random.nextInt(idx + 1);
      CSQueue ret = q.get(selected);
      Collections.swap(q, selected, idx--);
      return ret;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "Unsupported remove for " + this.getClass().getCanonicalName());
    }

    @VisibleForTesting
    protected void setRandom(Random r) {
      this.random = r;
    }
  }
}
