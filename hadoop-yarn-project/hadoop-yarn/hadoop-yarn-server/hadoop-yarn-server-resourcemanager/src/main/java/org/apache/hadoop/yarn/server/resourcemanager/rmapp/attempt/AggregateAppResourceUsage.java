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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class AggregateAppResourceUsage {
  long memory;
  int vcores;

  long memorySeconds;
  long vcoreSeconds;

  public AggregateAppResourceUsage(long memory, int vcores, long memorySeconds, long vcoreSeconds) {
    this.memory = memory;
    this.vcores = vcores;
    this.memorySeconds = memorySeconds;
    this.vcoreSeconds = vcoreSeconds;
  }

  /**
   * @return the memory
   */
  public long getMemory() {
    return memory;
  }

  /**
   * @param memory the memory to set
   */
  public void setMemory(long memory) {
    this.memory = memory;
  }

  /**
   * @return the vcores
   */
  public int getVcores() {
    return vcores;
  }

  /**
   * @param vcores the vcores to set
   */
  public void setVcores(int vcores) {
    this.vcores = vcores;
  }

  /**
   * @return the memorySeconds
   */
  public long getMemorySeconds() {
    return memorySeconds;
  }

  /**
   * @param memorySeconds the memorySeconds to set
   */
  public void setMemorySeconds(long memorySeconds) {
    this.memorySeconds = memorySeconds;
  }

  /**
   * @return the vcoreSeconds
   */
  public long getVcoreSeconds() {
    return vcoreSeconds;
  }

  /**
   * @param vcoreSeconds the vcoreSeconds to set
   */
  public void setVcoreSeconds(long vcoreSeconds) {
    this.vcoreSeconds = vcoreSeconds;
  }
}
