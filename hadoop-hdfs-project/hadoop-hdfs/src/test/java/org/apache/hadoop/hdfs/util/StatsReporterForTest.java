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

import com.google.common.collect.ImmutableSortedMap;
import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.Capabilities;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

/**
 * A StatsReporter implementation for testing MetricsPublisher
 */
public class StatsReporterForTest implements StatsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(StatsReporterForTest.class);
  private static final Map<String, String> EMPTY_TAGS = Collections.emptyMap();

  private final Map<String, Long> counterMap;
  private final Map<String, Double> gaugeMap;

  public StatsReporterForTest(Map<String, Long> counterMap, Map<String, Double> gaugeMap) {
    this.counterMap = counterMap;
    this.gaugeMap = gaugeMap;
  }

  public String getKey(String name, Map<String, String> tags) {
    SortedMap<String, String> sortedMap = ImmutableSortedMap.copyOf(tags);
    return name + ":" + sortedMap.toString();
  }

  @Override
  public void reportCounter(String name, Map<String, String> tags, long value) {
    Map<String, String> reportedTags = tags == null ? EMPTY_TAGS : tags;
    LOG.info("reportCounter: " + name + ":" + reportedTags + ":" + value);
    final String key = getKey(name, reportedTags);
    if (counterMap.containsKey(key)) {
      counterMap.put(key, counterMap.get(key) + value);
    } else {
      counterMap.put(key, value);
    }
  }

  @Override
  public void reportGauge(String name, Map<String, String> tags, double value) {
    Map<String, String> reportedTags = tags == null ? EMPTY_TAGS : tags;
    LOG.info("reportGauge: " + name + ":" + reportedTags + ":" + value);
    final String key = getKey(name, reportedTags);
    if (gaugeMap.containsKey(key)) {
      gaugeMap.put(key, gaugeMap.get(key) + value);
    } else {
      gaugeMap.put(key, value);
    }
  }

  @Override
  public void reportTimer(String name, Map<String, String> tags, Duration interval) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reportHistogramValueSamples(String name, Map<String, String> tags, Buckets buckets, double bucketLowerBound, double bucketUpperBound, long samples) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void reportHistogramDurationSamples(String name, Map<String, String> tags, Buckets buckets, Duration bucketLowerBound, Duration bucketUpperBound, long samples) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Capabilities capabilities() {
    return null;
  }

  @Override
  public void flush() {
    LOG.info("StatsReporterForTest is flushed");
  }

  @Override
  public void close() {
    LOG.info("StatsReporterForTest is closed");
  }
}
