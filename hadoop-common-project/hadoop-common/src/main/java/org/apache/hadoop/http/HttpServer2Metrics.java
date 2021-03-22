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
package org.apache.hadoop.http;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.mortbay.jetty.handler.StatisticsHandler;

/**
 * This class collects all the metrics of Jetty's StatisticsHandler
 * and expose them as Hadoop Metrics.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(name="HttpServer2", about="HttpServer2 metrics", context="http")
public class HttpServer2Metrics {

  private final StatisticsHandler handler;
  private final int port;

  @Metric("number of requests")
  public int requests() {
    return handler.getRequests();
  }

  @Metric("number of requests currently active")
  public int requestsActive() {
    return handler.getRequestsActive();
  }

  @Metric("maximum number of active requests")
  public int requestsActiveMax() {
    return handler.getRequestsActiveMax();
  }

  @Metric("maximum time spent handling requests (in ms)")
  public long requestTimeMax() {
    return handler.getRequestTimeMax();
  }

  @Metric("total time spent in all request handling (in ms)")
  public long requestTimeTotal() {
    return handler.getRequestTimeTotal();
  }

  @Metric("average time spent in all request handling (in ms)")
  public long requestTimeAverage() {
    return handler.getRequestTimeAverage();
  }

  @Metric("minimum time spent handling requests (in ms)")
  public long requestTimeMin() {
    return handler.getRequestTimeMin();
  }

  @Metric("number of requests with 1xx response status")
  public int responses1xx() {
    return handler.getResponses1xx();
  }

  @Metric("number of requests with 2xx response status")
  public int responses2xx() {
    return handler.getResponses2xx();
  }

  @Metric("number of requests with 3xx response status")
  public int responses3xx() {
    return handler.getResponses3xx();
  }

  @Metric("number of requests with 4xx response status")
  public int responses4xx() {
    return handler.getResponses4xx();
  }

  @Metric("number of requests with 5xx response status")
  public int responses5xx() {
    return handler.getResponses5xx();
  }

  @Metric("time in milliseconds stats have been collected for")
  public long statsOnMs() {
    return handler.getStatsOnMs();
  }

  HttpServer2Metrics(StatisticsHandler handler, int port) {
    this.handler = handler;
    this.port = port;
  }

  static HttpServer2Metrics create(StatisticsHandler handler, int port) {
    final MetricsSystem ms = DefaultMetricsSystem.instance();
    final HttpServer2Metrics metrics = new HttpServer2Metrics(handler, port);
    // Remove the old metrics from metrics system to avoid duplicate error
    // when HttpServer2 is started twice.
    metrics.remove();
    // Add port number to the suffix to allow multiple instances in a host.
    return ms.register("HttpServer2-" + port, "HttpServer2 metrics", metrics);
  }

  void remove() {
    DefaultMetricsSystem.removeSourceName("HttpServer2-" + port);
  }
}

