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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.util.concurrent.ThreadLocalRandom;

/**
 * This class is for maintaining Datanode Volume IO related statistics and
 * publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@Metrics(name = "DataNodeVolume", about = "DataNode Volume metrics",
    context = "dfs")
public class DataNodeVolumeMetrics {
  private final MetricsRegistry registry = new MetricsRegistry("FsVolume");

  @Metric("number of blocks read from disk")
  private MutableCounterLong blocksReadFromDisk;
  @Metric("number of bytes read from disk")
  private MutableCounterLong bytesReadFromDisk;
  @Metric("number of bytes written to disk")
  private MutableCounterLong bytesWrittenToDisk;

  public long getBlocksReadFromDisk() {
    return blocksReadFromDisk.value();
  }

  public long getBytesReadFromDisk() {
    return bytesReadFromDisk.value();
  }

  public long getBytesWrittenToDisk() {
    return bytesWrittenToDisk.value();
  }

  public void incrBlocksReadFromDisk() {
    blocksReadFromDisk.incr();
  }

  public void incrBytesReadFromDisk(long delta) {
    bytesReadFromDisk.incr(delta);
  }

  public void incrBytesWrittenToDisk(long delta) {
    bytesWrittenToDisk.incr(delta);
  }

  private final String name;
  private final MetricsSystem ms;

  public DataNodeVolumeMetrics(final MetricsSystem metricsSystem,
      final String volumeName) {
    this.ms = metricsSystem;
    this.name = volumeName;
    registry.tag("volumeName", "volumeName", volumeName.replace('/', '_'));
  }

  public static DataNodeVolumeMetrics create(final Configuration conf,
      final String volumeName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    String name = "DataNodeVolume-"+ (volumeName.isEmpty()
        ? "UndefinedDataNodeVolume"+ ThreadLocalRandom.current().nextInt()
        : volumeName.replace(':', '-'));

    return ms.register(name, null, new DataNodeVolumeMetrics(ms, name));
  }

  public String name() {
    return name;
  }

  public void unRegister() {
    ms.unregisterSource(name);
  }
}