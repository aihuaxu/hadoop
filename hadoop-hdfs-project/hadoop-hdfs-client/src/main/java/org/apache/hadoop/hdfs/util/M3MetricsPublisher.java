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

import com.google.common.annotations.VisibleForTesting;
import com.uber.m3.tally.DurationBuckets;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.ScopeBuilder;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.tally.m3.M3Reporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe metrics publisher that publishes hdfs client metrics to M3.
 */
public class M3MetricsPublisher implements MetricsPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(M3MetricsPublisher.class);
  private static M3MetricsPublisher INSTANCE;

  private static final String SERVICE_NAME = "hadoop";
  private static final String DATANODE_TAG = "datanode";
  private static final String UBER_ENVIRONMENT = "UBER_ENVIRONMENT";
  // If client doesn't configure an environment name, use the UBER_ENVIRONMENT
  // system environment variable but add a prefix so we can easily identify them
  private static final String UBER_ENVIRONMENT_PREFIX = "uber_env_";
  private static final int SHUTDOWN_HOOK_PRIORITY = 10;

  private static final String SCHEMA_TAG = "schema";
  private static final String DEFAULT_BUCKET_SCHEMA = "default_v1";
  private static final long MILLISECONDS_PER_SECOND = 1000;
  private static final long MILLISECONDS_PER_MINUTE = 60 * MILLISECONDS_PER_SECOND;
  private static final long MILLISECONDS_PER_HOUR = 60 * MILLISECONDS_PER_MINUTE;
  private static final long[] DEFAULT_DURATION_BUCKETS =
    {
      10 * MILLISECONDS_PER_SECOND,  20 * MILLISECONDS_PER_SECOND,
      40 * MILLISECONDS_PER_SECOND,
      1  * MILLISECONDS_PER_MINUTE,  2  * MILLISECONDS_PER_MINUTE,
      5 * MILLISECONDS_PER_MINUTE,   10 * MILLISECONDS_PER_MINUTE,
      30 * MILLISECONDS_PER_MINUTE,  1  * MILLISECONDS_PER_HOUR
    };

  // Metrics name and schema to Histogram
  private ConcurrentHashMap<String, Histogram> histogramMap = new ConcurrentHashMap<>();

  /**
   * Parent M3 scope for metrics with a "datanode" tag.
   * When datanode tag is enabled, there will be NO tag for client host
   * otherwise the total cardinality would be too large for M3 to handle
   */
  private final Scope dnParentScope;

  /**
   * General scope with a "host" tag.
   */
  private final Scope scope;


  public static synchronized M3MetricsPublisher getInstance(DfsClientConf conf) {
    if (INSTANCE == null) {
      INSTANCE = new M3MetricsPublisher(conf);
      ShutdownHookManager.get().addShutdownHook(shutdownHook, SHUTDOWN_HOOK_PRIORITY);
    }
    return INSTANCE;
  }

  private M3MetricsPublisher(DfsClientConf conf) {
    dnParentScope = createM3Client(false, conf);
    scope = createM3Client(true, conf);
  }

  private static final Runnable shutdownHook = new Runnable() {
    @Override
    public void run() {
      if (INSTANCE != null) {
        INSTANCE.close();
      }
    }
  };


  @VisibleForTesting
  public void closeForTest() {
    close();
  }

  private void close() {
    try {
      if (dnParentScope != null) {
        dnParentScope.close();
      }
      if (scope != null) {
        scope.close();
      }
    } catch (Exception e) {
      LOG.warn("Could not close metrics clients gracefully", e);
    }
  }

  @Override
  public void counter(String name, long amount) {
    if (preEmitCheck()) {
      scope.counter(name).inc(amount);
    }
  }

  /**
   * Emit client->datanode metrics with the "datanode" tag.
   * @param datanode the host name of the datanode
   */
  public void counter(String datanode, String name, long amount) {
    if (!preEmitCheck()) {
      return;
    }
    if (datanode == null || datanode.isEmpty()) {
      LOG.warn("counter is called with empty datanode.");
      return;
    }
    Scope dnScope = getDatanodeScope(datanode);
    dnScope.counter(name).inc(amount);
  }

  @Override
  public void gauge(String name, long amount) {
    if (preEmitCheck()) {
      scope.gauge(name).update(amount);
    }
  }

  /**
   * Emit client->datanode metrics with the "datanode" tag.
   * @param datanode the host name of the datanode
   */
  @Override
  public void gauge(String datanode, String name, long amount) {
    if (!preEmitCheck()) {
      return;
    }
    if (datanode == null || datanode.isEmpty()) {
      LOG.warn("gauge is called with empty datanode.");
      return;
    }
    Scope dnScope = getDatanodeScope(datanode);
    dnScope.gauge(name).update(amount);
  }

  /**
   * Histogram is mainly for time durations.
   * @param duration in milliseconds
   */
  @Override
  public void histogram(String name, long duration) {
    histogram(name, duration, DEFAULT_BUCKET_SCHEMA, DEFAULT_DURATION_BUCKETS);
  }

  /**
   * "Schema must be bumped if bucket is changed to namespace the different buckets
   * from each other so they don't overlap and cause the histogram function to error out
   * due to overlapping buckets in the same query." -- M3
   */
  private void histogram(String name, long duration, String schema, long[] bucket) {
    if (!preEmitCheck()) {
      return;
    }

    StringBuilder keyBuilder = new StringBuilder(name);
    keyBuilder.append(':');
    keyBuilder.append(schema);
    String key = keyBuilder.toString();

    Histogram histogram = histogramMap.get(key);
    if (histogram == null) {
      Duration[] durations = new Duration[bucket.length];
      for (int i = 0; i < durations.length; i++) {
        durations[i] = Duration.ofMillis(bucket[i]);
      }
      histogram = scope.tagged(Collections.singletonMap(SCHEMA_TAG, schema))
        .histogram(name, new DurationBuckets(durations));
      histogramMap.putIfAbsent(key, histogram);

      // Neither the existing histogram in the map nor the one we just created can be closed.
      // M3 caches subscopes and histograms, so they are actually the same object.
      // Closing any of them will lead to a handicapped subscope and histogram
    }
    histogram.recordDuration(Duration.ofMillis(duration));
  }

  private boolean preEmitCheck() {
    if (scope == null || dnParentScope == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("scope=" + scope + ", dnParentScope=" + dnParentScope);
      }
      return false;
    }
    return true;
  }

  private Scope getDatanodeScope(String datanode) {
    return dnParentScope.tagged(Collections.singletonMap(DATANODE_TAG, datanode));
  }

  private static Scope createM3Client(boolean includeHost, DfsClientConf conf) {
    final InetSocketAddress reporterAddr =
      getReporterAddress(conf.getMetricsReporterAddr());
    final long reportInterval = conf.getMetricsReportIntervalMs();
    final String metricsEnvironment = getMetricsEnvironment(conf);

    ImmutableMap.Builder<String, String> tagsBuilder = new ImmutableMap.Builder<>();
    tagsBuilder.put(M3Reporter.SERVICE_TAG, SERVICE_NAME);
    tagsBuilder.put(M3Reporter.ENV_TAG, metricsEnvironment);
    tagsBuilder.put("language", "java");
    String version = MetricsPublisher.class.getPackage().getImplementationVersion();
    if (version != null) {
      tagsBuilder.put("version", version);
    }

    try {
      StatsReporter reporter = new M3Reporter.Builder(reporterAddr)
        .includeHost(includeHost)
        .commonTags(tagsBuilder.build())
        .build();

      ScopeBuilder scopeBuilder = new RootScopeBuilder().reporter(reporter);
      return scopeBuilder.reportEvery(Duration.ofMillis(reportInterval));
    } catch (Exception e) {
      LOG.error("Unable to initialize m3 client.", e);
      return null;
    }
  }

  private static InetSocketAddress getReporterAddress(String reporterAddrStr) {
    String[] splits = reporterAddrStr.trim().split(":");
    String hostname = splits[0];
    int port = Integer.parseInt(splits[1]);
    return new InetSocketAddress(hostname, port);
  }

  private static String getMetricsEnvironment(DfsClientConf conf) {
    String metricsEnvironment = conf.getMetricsEnvironment();
    if (metricsEnvironment != null && !metricsEnvironment.isEmpty()) {
      return metricsEnvironment;
    }

    metricsEnvironment = UBER_ENVIRONMENT_PREFIX;
    String uberEnvironment = System.getenv().get(UBER_ENVIRONMENT);
    if (uberEnvironment != null && !uberEnvironment.isEmpty()) {
      metricsEnvironment += uberEnvironment;
    }

    return metricsEnvironment;
  }
}
