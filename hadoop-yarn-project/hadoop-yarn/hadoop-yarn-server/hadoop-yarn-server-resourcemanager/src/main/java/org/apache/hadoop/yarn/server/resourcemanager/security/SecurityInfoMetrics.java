package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

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

@InterfaceAudience.Private
@Metrics(context="yarn")
public class SecurityInfoMetrics {
    static final Logger LOG = LoggerFactory.getLogger(SecurityInfoMetrics.class);
    private static AtomicBoolean isInitialized = new AtomicBoolean(false);

    @Metric("# of YARN RM cancel delegation tokens") MutableGaugeLong sizeDelegationTokenCancel;
    @Metric("# of failed renew KMS delegation token ops") MutableCounterLong numFailedKMSTokenRenew;
    @Metric("# of RM DelegationTokenRenewer futures") MutableGaugeLong dtrNumFutures;
    @Metric("# of RM DelegationTokenRenewer pendingEventQueue") MutableGaugeLong dtrNumPendingEventQueue;

    @Metric("DelegationTokenRenewerLoopTracker timeout check running time") MutableCounterLong dtrTimeoutCheckRunningTime;
    @Metric("# of RM DelegationTokenRenewer futures completed") MutableCounterLong dtrNumFutureCompleted;
    @Metric("# of RM DelegationTokenRenewer futures not started") MutableCounterLong dtrNumFutureNotStarted;
    @Metric("# of RM DelegationTokenRenewer timeout") MutableCounterLong dtrNumFutureTimeout;
    @Metric("# of RM DelegationTokenRenewer timeout and need to retry") MutableCounterLong dtrNumFutureTimeoutNeedRetry;
    @Metric("# of RM DelegationTokenRenewer timeout and will not retry") MutableCounterLong dtrNumFutureTimeoutWithoutRetry;
    @Metric("# of RM DelegationTokenRenewer timeout exceeded max retries") MutableCounterLong dtrNumFutureTimeoutExceedMaxRetries;
    @Metric("# of RM DelegationTokenRenewer timeout check exceptions") MutableCounterLong dtrNumTimeoutCheckExceptions;

    @Metric("DelegationTokenRenewerLoopTracker timeout check running time") MutableCounterLong dtrNumRejectApps;

    static final MetricsInfo RECORD_INFO = info("SecurityInfoMetrics",
            "Metrics for YARN RM Delegation Tokens");

    static SecurityInfoMetrics INSTANCE = null;
    private static MetricsRegistry registry;

    public static SecurityInfoMetrics getMetrics() {
        if(!isInitialized.get()){
            synchronized (SecurityInfoMetrics.class) {
                if(INSTANCE == null){
                    INSTANCE = new SecurityInfoMetrics();
                    registerMetrics();
                    isInitialized.set(true);
                }
            }
        }
        return INSTANCE;
    }

    public synchronized static void registerMetrics() {
        registry = new MetricsRegistry(RECORD_INFO);
        registry.tag(RECORD_INFO, "ResourceManager");
        MetricsSystem ms = DefaultMetricsSystem.instance();
        if (ms != null) {
            ms.register("SecurityInfoMetrics", "Metrics for YARN RM Delegation Tokens", INSTANCE);
        } else {
            LOG.warn("Failed to initialize SecurityInfoMetrics because DefaultMetricsSystem is null.");
        }
    }

    public synchronized void setSizeDelegationTokenCancel(int num) {
        sizeDelegationTokenCancel.set(num);
    }

    public long getSizeDelegationTokenCancel() {
        return sizeDelegationTokenCancel.value();
    }

    public synchronized void incrementNumFailedKMSRenew() {
        numFailedKMSTokenRenew.incr();
    }

    public synchronized void setDtrNumFutures(int size) {
        dtrNumFutures.set(size);
    }

    public synchronized void setDtrNumPendingEventQueue(int size) {
        dtrNumPendingEventQueue.set(size);
    }

    public synchronized void incrementDtrTimeoutCheckRunningTime(long runningTime) {
        dtrTimeoutCheckRunningTime.incr(runningTime);
    }

    public synchronized void incrDtrNumFutureCompleted(int num) {
        dtrNumFutureCompleted.incr(num);
    }

    public synchronized void incrDtrNumFutureNotStarted(int num) {
        dtrNumFutureNotStarted.incr(num);
    }

    public synchronized void incrDtrNumFutureTimeout(int num) {
        dtrNumFutureTimeout.incr(num);
    }

    public synchronized void incrDtrNumFutureTimeoutWithoutRetry(int num) {
        dtrNumFutureTimeoutWithoutRetry.incr(num);
    }

    public synchronized void incrDtrNumFutureTimeoutNeedRetry(int num) {
        dtrNumFutureTimeoutNeedRetry.incr(num);
    }

    public synchronized void incrDtrNumFutureTimeoutExceedMaxRetries(int num) {
        dtrNumFutureTimeoutExceedMaxRetries.incr(num);
    }

    public synchronized void incrementDtrNumTimeoutCheckExceptions() {
        dtrNumTimeoutCheckExceptions.incr();
    }

    public synchronized void incrDtrNumRejectApps() {
        dtrNumRejectApps.incr();
    }

}
