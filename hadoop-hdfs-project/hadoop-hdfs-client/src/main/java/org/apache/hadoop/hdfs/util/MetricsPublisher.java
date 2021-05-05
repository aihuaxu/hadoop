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
package org.apache.hadoop.hdfs.util;



public interface MetricsPublisher {
  void counter(String name, long amount);

  /**
   * Emit client->datanode metrics with the "datanode" tag.
   * @param datanode the host name of the datanode
   */
  void counter(String datanode, String name, long amount);

  void gauge(String name, long amount);

  /**
   * Emit client->datanode metrics with the "datanode" tag.
   * @param datanode the host name of the datanode
   */
  void gauge(String datanode, String name, long amount);

  /**
   * Histogram is mainly for time durations.
   * @param duration in milliseconds
   */
  void histogram(String name, long duration);
}
