/*
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
package org.apache.hadoop.yarn.server.router.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * This class is for maintaining the various Router Federation Interceptor
 * activity statistics and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about = "Metrics for Router RPC", context = "yarnrouter")
public final class RouterRpcMetrics {

  private static final String METRIC_NAME = "RouterRPCMetrics";
  private static final MetricsInfo RECORD_INFO =
      info(METRIC_NAME, "Router PRC");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  // Metrics for operation failed
  @Metric("# of applications failed to be submitted")
  private MutableGaugeInt numAppsFailedSubmitted;
  @Metric("# of applications failed to be created")
  private MutableGaugeInt numAppsFailedCreated;
  @Metric("# of applications failed to be killed")
  private MutableGaugeInt numAppsFailedKilled;
  @Metric("# of application reports failed to be retrieved")
  private MutableGaugeInt numAppsFailedRetrieved;
  @Metric("# of multiple applications reports failed to be retrieved")
  private MutableGaugeInt numMultipleAppsFailedRetrieved;
  @Metric("# of delegation token to be retrieved")
  private MutableGaugeInt numDTFailedRetrieved;
  @Metric("# of delegation token to be renewed")
  private MutableGaugeInt numDTFailedRenewed;
  @Metric("# of delegation token to be cancelled")
  private MutableGaugeInt numDTFailedCancelled;

  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Total number of successful Submitted apps and latency(ms)")
  private MutableRate totalSucceededAppsSubmitted;
  @Metric("Total number of successful Killed apps and latency(ms)")
  private MutableRate totalSucceededAppsKilled;
  @Metric("Total number of successful Created apps and latency(ms)")
  private MutableRate totalSucceededAppsCreated;
  @Metric("Total number of successful Retrieved app reports and latency(ms)")
  private MutableRate totalSucceededAppsRetrieved;
  @Metric("Total number of successful Retrieved multiple apps reports and "
      + "latency(ms)")
  private MutableRate totalSucceededMultipleAppsRetrieved;
  @Metric("Total number of successful delegation token retrieved")
  private MutableRate totalSucceededDTRetrieved;
  @Metric("Total number of successful delegation token renewed")
  private MutableRate totalSucceededDTRenewed;
  @Metric("Total number of successful delegation token cancelled")
  private MutableRate totalSucceededDTCancelled;

  @Metric("Time for the router to process an operation internally")
  private MutableRate processing;
  @Metric("Number of operations the Router processed internally")
  private MutableCounterLong processingOp;
  @Metric("Time for the Router to proxy an operation to the RM")
  private MutableRate proxy;
  @Metric("Number of operations the Router proxied to a RM")
  private MutableCounterLong proxyOpSucceed;
  @Metric("Number of operations to fail to reach RM")
  private MutableCounterLong proxyOpFailure;

  @Metric("Number of current Delegation Token count")
  private MutableGaugeLong currentDTCount;

  /**
   * Provide quantile counters for all latencies.
   */
  private MutableQuantiles submitApplicationLatency;
  private MutableQuantiles getNewApplicationLatency;
  private MutableQuantiles killApplicationLatency;
  private MutableQuantiles getApplicationReportLatency;
  private MutableQuantiles getApplicationsReportLatency;
  private MutableQuantiles retrieveDTLatency;
  private MutableQuantiles renewDTLatency;
  private MutableQuantiles cancelDTLatency;

  private static volatile RouterRpcMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  private RouterRpcMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "Router");
    getNewApplicationLatency = registry.newQuantiles("getNewApplicationLatency",
        "latency of get new application", "ops", "latency", 10);
    submitApplicationLatency = registry.newQuantiles("submitApplicationLatency",
        "latency of submit application", "ops", "latency", 10);
    killApplicationLatency = registry.newQuantiles("killApplicationLatency",
        "latency of kill application", "ops", "latency", 10);
    getApplicationReportLatency =
        registry.newQuantiles("getApplicationReportLatency",
            "latency of get application report", "ops", "latency", 10);
    getApplicationsReportLatency =
        registry.newQuantiles("getApplicationsReportLatency",
            "latency of get applications report", "ops", "latency", 10);
    retrieveDTLatency =
        registry.newQuantiles("retrieveDTLatency",
            "latency of retrieve Delegation token", "ops", "latency", 10);
    renewDTLatency =
        registry.newQuantiles("renewDTLatency",
            "latency of renew Delegation token", "ops", "latency", 10);
    cancelDTLatency =
        registry.newQuantiles("cancelDTLatency",
            "latency of cancel Delegation token", "ops", "latency", 10);
  }

  public static RouterRpcMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (RouterRpcMetrics.class) {
        if (INSTANCE == null) {
          INSTANCE = DefaultMetricsSystem.instance().register(METRIC_NAME,
              "Metrics for the Yarn Router interceptor", new RouterRpcMetrics());
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  @VisibleForTesting
  public long getNumSucceededAppsCreated() {
    return totalSucceededAppsCreated.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsSubmitted() {
    return totalSucceededAppsSubmitted.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsKilled() {
    return totalSucceededAppsKilled.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsRetrieved() {
    return totalSucceededAppsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededMultipleAppsRetrieved() {
    return totalSucceededMultipleAppsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsCreated() {
    return totalSucceededAppsCreated.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsSubmitted() {
    return totalSucceededAppsSubmitted.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsKilled() {
    return totalSucceededAppsKilled.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppReport() {
    return totalSucceededAppsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededMultipleGetAppReport() {
    return totalSucceededMultipleAppsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public int getAppsFailedCreated() {
    return numAppsFailedCreated.value();
  }

  @VisibleForTesting
  public int getAppsFailedSubmitted() {
    return numAppsFailedSubmitted.value();
  }

  @VisibleForTesting
  public int getAppsFailedKilled() {
    return numAppsFailedKilled.value();
  }

  @VisibleForTesting
  public int getAppsFailedRetrieved() {
    return numAppsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getMultipleAppsFailedRetrieved() {
    return numMultipleAppsFailedRetrieved.value();
  }

  @VisibleForTesting
  public long getProcessingOps() {
    return processingOp.value();
  }

  public long getProxyOpFailure() {
    return proxyOpFailure.value();
  }

  public long getCurrentDTCount() {
    return currentDTCount.value();
  }

  public void setCurrentDTCount(long value) {
    currentDTCount.set(value);
  }

  public void succeededAppsCreated(long duration) {
    totalSucceededAppsCreated.add(duration);
    getNewApplicationLatency.add(duration);
  }

  public void succeededAppsSubmitted(long duration) {
    totalSucceededAppsSubmitted.add(duration);
    submitApplicationLatency.add(duration);
  }

  public void succeededAppsKilled(long duration) {
    totalSucceededAppsKilled.add(duration);
    killApplicationLatency.add(duration);
  }

  public void succeededAppsRetrieved(long duration) {
    totalSucceededAppsRetrieved.add(duration);
    getApplicationReportLatency.add(duration);
  }

  public void succeededMultipleAppsRetrieved(long duration) {
    totalSucceededMultipleAppsRetrieved.add(duration);
    getApplicationsReportLatency.add(duration);
  }

  public void succeededDTRetrieved(long duration) {
    totalSucceededDTRetrieved.add(duration);
    retrieveDTLatency.add(duration);
  }

  public void succeededDTRenewed(long duration) {
    totalSucceededDTRenewed.add(duration);
    renewDTLatency.add(duration);
  }

  public void succeededDTCancelled(long duration) {
    totalSucceededDTCancelled.add(duration);
    cancelDTLatency.add(duration);
  }

  public void incrAppsFailedCreated() {
    numAppsFailedCreated.incr();
  }

  public void incrAppsFailedSubmitted() {
    numAppsFailedSubmitted.incr();
  }

  public void incrAppsFailedKilled() {
    numAppsFailedKilled.incr();
  }

  public void incrAppsFailedRetrieved() {
    numAppsFailedRetrieved.incr();
  }

  public void incrMultipleAppsFailedRetrieved() {
    numMultipleAppsFailedRetrieved.incr();
  }

  public void incrProxyOpFailure() {
    proxyOpFailure.incr();
  }

  public void incrDTRetrievedFailure() {
    numDTFailedRetrieved.incr();
  }

  public void incrDTRenewFailure() {
    numDTFailedRenewed.incr();
  }

  public void incrDTCancelFailure() {
    numDTFailedCancelled.incr();
  }

  /**
   * Add the time to process a request in the Router from the time we receive
   * the call until we send it to the RMs.
   * @param time Process time of an operation in milliseconds.
   */
  public void addProcessingTime(long time) {
    processing.add(time);
    processingOp.incr();
  }

  /**
   * Add the time to proxy an operation from the moment the Router sends it to
   * the RM until it replied.
   * @param time Proxy time of an operation in milliseconds.
   */
  public void addProxyTime(long time) {
    proxy.add(time);
    proxyOpSucceed.incr();
  }

}