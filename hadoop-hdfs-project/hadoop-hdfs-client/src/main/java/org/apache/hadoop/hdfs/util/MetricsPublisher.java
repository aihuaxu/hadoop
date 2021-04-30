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

/**
 * A thread-safe metrics publisher that publishes hdfs client metrics to M3.
 */
public class MetricsPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsPublisher.class);
  private static MetricsPublisher INSTANCE;

  private static final String SERVICE_NAME = "hadoop";
  private static final String DATANODE_TAG = "datanode";
  private static final String UBER_ENVIRONMENT = "UBER_ENVIRONMENT";
  // If client doesn't configure an environment name, use the UBER_ENVIRONMENT
  // system environment variable but add a prefix so we can easily identify them
  private static final String UBER_ENVIRONMENT_PREFIX = "uber_env_";
  private static final int SHUTDOWN_HOOK_PRIORITY = 10;

  public enum MetricType {
    GAUGE,
    COUNTER
  }

  public static synchronized MetricsPublisher getInstance(DfsClientConf conf) {
    if (INSTANCE == null) {
      INSTANCE = new MetricsPublisher(conf);
      ShutdownHookManager.get().addShutdownHook(shutdownHook, SHUTDOWN_HOOK_PRIORITY);
    }
    return INSTANCE;
  }

  private static final Runnable shutdownHook = new Runnable() {
    @Override
    public void run() {
      if (INSTANCE != null) {
        INSTANCE.close();
      }
    }
  };

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

  private MetricsPublisher(DfsClientConf conf) {
    dnParentScope = createM3Client(false, conf);
    scope = createM3Client(true, conf);
  }

  private static InetSocketAddress getReporterAddress(String reporterAddrStr) {
    String[] splits = reporterAddrStr.trim().split(":");
    String hostname = splits[0];
    int port = Integer.parseInt(splits[1]);
    return new InetSocketAddress(hostname, port);
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

  @VisibleForTesting
  public void closeForTest() {
    close();
  }

  /**
   * Emit client->datanode metrics with the "datanode" tag.
   * @param datanode the host name of the datanode
   */
  public void emit(MetricType metricType, String datanode,
                            String name, long amount) {
    if (datanode == null || datanode.length() == 0) {
      LOG.warn("emit is called with empty datanode.");
      return;
    }
    if (dnParentScope == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("MetricsPublisher#dnParentScope is null");
      }
      return;
    }

    Scope dnScope = getDatanodeScope(datanode);
    switch (metricType) {
      case GAUGE:
        dnScope.gauge(name).update(amount);
        break;
      case COUNTER:
        dnScope.counter(name).inc(amount);
        break;
    }
  }

  private Scope getDatanodeScope(String datanode) {
    return dnParentScope.tagged(Collections.singletonMap(DATANODE_TAG, datanode));
  }

  public void emit(MetricType metricType, String name, long amount) {
    if (scope == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("MetricsPublisher#scope is null");
      }
      return;
    }
    switch (metricType) {
      case GAUGE:
        scope.gauge(name).update(amount);
        break;
      case COUNTER:
        scope.counter(name).inc(amount);
        break;
    }
  }

  private static Scope createM3Client(boolean includeHost, DfsClientConf conf) {
    final InetSocketAddress reporterAddr =
        getReporterAddress(conf.getMetricsReporterAddr());
    final long reportInterval = conf.getMetricsReportIntervalMs();
    final String metricsEnvironment = getMetricsEnvironment(conf);

    ImmutableMap.Builder<String, String> tagsBuilder = new ImmutableMap.Builder<>();
    tagsBuilder.put(M3Reporter.SERVICE_TAG, SERVICE_NAME);
    tagsBuilder.put(M3Reporter.ENV_TAG, metricsEnvironment);

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
