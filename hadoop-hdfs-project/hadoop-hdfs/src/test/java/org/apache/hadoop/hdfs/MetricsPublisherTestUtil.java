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

import com.uber.m3.tally.Scope;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.util.M3MetricsPublisher;
import org.apache.hadoop.hdfs.util.MetricsPublisher;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsPublisherTestUtil {
  public static class MetricsTestSink {
    private final Map<String, Long> counterMap = new ConcurrentHashMap<>();
    private final Map<String, Double> gaugeMap = new ConcurrentHashMap<>();

    public boolean isCounterEmpty() {
      return counterMap.isEmpty();
    }

    public boolean isGaugeEmpty() {
      return gaugeMap.isEmpty();
    }

    public long getCounterValue(String name) {
      final String key = name + ":{}";
      Long value = counterMap.get(key);
      return value == null ? 0 : value;
    }

    public double getGaugeValue(String name) {
      final String key = name + ":{}";
      Double value = gaugeMap.get(key);
      return value == null ? 0.0 : value;
    }

    public long getCounterValue(String name, String nodeTag) {
      String key = name + ":" + Collections.singletonMap("datanode", nodeTag);
      Long value = counterMap.get(key);
      return value == null ? 0 : value;
    }

    public double getGaugeValue(String name, String nodeTag) {
      String key = name + ":" + Collections.singletonMap("datanode", nodeTag);
      Double value = gaugeMap.get(key);
      return value == null ? 0.0 : value;
    }

    public void clear() {
      counterMap.clear();
      gaugeMap.clear();
    }
  }

  public static MetricsTestSink createTestMetricsPublisher(DfsClientConf conf,
          long reportInterval) throws Exception {
    MetricsTestSink sink = new MetricsTestSink();
    MetricsPublisher instance = M3MetricsPublisher.getInstance(conf);
    final Scope dnParentScope = DFSTestUtil.createM3ClientForTest(
            reportInterval, sink.counterMap, sink.gaugeMap);
    final Scope scope = DFSTestUtil.createM3ClientForTest(
            reportInterval, sink.counterMap, sink.gaugeMap);

    Field dnScopeField = M3MetricsPublisher.class.getDeclaredField("dnParentScope");
    dnScopeField.setAccessible(true);
    dnScopeField.set(instance, dnParentScope);

    Field scopeField = M3MetricsPublisher.class.getDeclaredField("scope");
    scopeField.setAccessible(true);
    scopeField.set(instance, scope);

    return sink;
  }
}
