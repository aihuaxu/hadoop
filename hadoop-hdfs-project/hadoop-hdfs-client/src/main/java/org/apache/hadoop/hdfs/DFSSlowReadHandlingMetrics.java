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
package org.apache.hadoop.hdfs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The client-side metrics for hedged read feature.
 * This class has a number of metrics variables that are publicly accessible,
 * we can grab them from client side, like HBase.
 */
@InterfaceAudience.Private
public class DFSSlowReadHandlingMetrics {
  /**
   * Names of the metrics related to slowness in BlockReader
   */
  @VisibleForTesting
  public static final String SLOW_PACKET_TIME = "client.blockreader.slow_packet_time";
  public static final String NUM_SLOW_PACKET = "client.blockreader.num_slow_packet";
  public static final String SLOW_PACKET_TIME_SOURCE = "client.blockreader.slow_packet_time_source";

  /**
   * Names of the slow read metrics
   */
  static final String SLOW_READ_DIST = "client.slow_read_distribution";
  static final String SLOW_READ_TIME = "client.slow_read_time";
  static final String NUM_SLOW_READ = "client.num_slow_read";
  static final String SLOW_PREAD_DIST = "client.slow_pread_distribution";
  static final String SLOW_PREAD_TIME = "client.slow_pread_time";
  static final String NUM_SLOW_PREAD = "client.num_slow_pread";
  static final String SLOW_BLOCKREADER_CREATION = "client.slow_blockreader_creation";

  /**
   * Names of the fast-switch read metrics
   */
  static final String FAST_SWITCH_SWITCH_COUNT = "client.fast_switch_switch_count";
  static final String FAST_SWITCH_TOO_MANY_SLOWNESS_COUNT = "client.fast_switch_too_many_slowness_count";
  static final String FAST_SWITCH_ACTIVE_THREAD_COUNT = "client.fast_switch_active_thread_count";
  static final String FAST_SWITCH_TIMEOUT_COUNT = "client.fast_switch_timeout_count";
  static final String FAST_SWITCH_THREAD_SLOW_START_COUNT = "client.fast_switch_thread_slow_start";
  static final String READ_THREADPOOL_REJECTION_COUNT = "client.read_thread_pool_rejection_count";

  public final AtomicLong hedgedReadOps = new AtomicLong();
  public final AtomicLong hedgedReadOpsWin = new AtomicLong();
  public final AtomicLong readOpsInCurThread = new AtomicLong();

  public void incHedgedReadOps() {
    hedgedReadOps.incrementAndGet();
  }

  public void incReadOpsInCurThread() {
    readOpsInCurThread.incrementAndGet();
  }

  public void incHedgedReadWins() {
    hedgedReadOpsWin.incrementAndGet();
  }

  public long getHedgedReadOps() {
    return hedgedReadOps.longValue();
  }

  public long getReadOpsInCurThread() {
    return readOpsInCurThread.longValue();
  }

  public long getHedgedReadWins() {
    return hedgedReadOpsWin.longValue();
  }
}
