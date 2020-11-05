package org.apache.hadoop.yarn.server.router.metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.apache.hadoop.util.Time.monotonicNow;

public class RouterRPCPerformanceMonitor {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRPCPerformanceMonitor.class);


  /** Time for an operation to be received in the Router. */
  private static final ThreadLocal<Long> START_TIME = new ThreadLocal<>();
  /** Time for an operation to be send to the Resourcemanager. */
  private static final ThreadLocal<Long> PROXY_TIME = new ThreadLocal<>();

  /** JMX interface to monitor the RPC metrics. */
  private RouterRpcMetrics metrics;

  /** Thread pool for logging stats. */
  private ExecutorService executor;

  public RouterRPCPerformanceMonitor() {}

  public void init() {
    // Create metrics
    this.metrics = RouterRpcMetrics.getMetrics();

    // Create thread pool
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Federation RPC Performance Monitor-%d").build();
    this.executor = Executors.newFixedThreadPool(1, threadFactory);
  }

  public void close() {
    if (this.executor != null) {
      this.executor.shutdown();
    }
  }

  public void startOp() {
    START_TIME.set(this.getNow());
  }

  public void proxyOp() {
    PROXY_TIME.set(this.getNow());
    long processingTime = getProcessingTime();
    metrics.addProcessingTime(processingTime);
  }

  public void proxyOpComplete() {
    long proxyTime = getProxyTime();
    metrics.addProxyTime(proxyTime);
  }

  public void proxyOpFailed() {
    metrics.incrProxyOpFailure();
  }


  /**
   * Get time between we receiving the operation and sending it to the RM.
   * @return Processing time in milliseconds.
   */
  private long getProcessingTime() {
    if (START_TIME.get() != null && START_TIME.get() > 0 &&
        PROXY_TIME.get() != null && PROXY_TIME.get() > 0) {
      return PROXY_TIME.get() - START_TIME.get();
    }
    return -1;
  }

  /**
   * Get time between now and when the operation was forwarded to the RM.
   * @return Current proxy time in milliseconds.
   */
  private long getProxyTime() {
    if (PROXY_TIME.get() != null && PROXY_TIME.get() > 0) {
      return getNow() - PROXY_TIME.get();
    }
    return -1;
  }

  public RouterRpcMetrics getRPCMetrics() {
    return this.metrics;
  }

  /**
   * Get current time.
   * @return Current time in milliseconds.
   */
  private long getNow() {
    return monotonicNow();
  }

}
