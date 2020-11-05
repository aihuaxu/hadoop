package org.apache.hadoop.yarn.server.router.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@InterfaceAudience.Private
@Metrics(about = "Metrics for Router YoP", context = "yarnrouteryop")
public final class YoPMetrics {

  private static final String METRIC_NAME = "RouterYoPMetrics";
  private static final MetricsInfo RECORD_INFO =
      info(METRIC_NAME, "Router YoP");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  // Metrics for operation failed
  @Metric("# of totalConnectPelotonServices")
  private MutableRate totalSucceedConnectPelotonServices;
  @Metric("# of totalGetOrderedHosts")
  private MutableRate totalSucceedGetOrderedHostsFromRM;
  @Metric("# of totalSucceedReclaimHostOrder")
  private MutableRate totalSucceedReclaimHostOrder;
  @Metric("# of totalQueryNMJob")
  private MutableRate totalSucceedQueryNMJob;

  @Metric("Number of failed getOrderedHostsFromRM")
  private MutableGaugeLong numOfFailedGetOrderedHostsFromRM;
  @Metric("Number of failed setReclaimHostOrderRequest")
  private MutableGaugeLong numOfFailedSetReclaimHost;
  @Metric("Number of failed setReclaimHostOrderRequest")
  private MutableGaugeLong numOfFailedFetchPelotonCreds;
  @Metric("Number of failed queryJob")
  private MutableGaugeLong numOfFailedQueryJob;

  private static volatile YoPMetrics INSTANCE = null;
  private static MetricsRegistry registry;

  private YoPMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "Router YoP");
  }

  public static YoPMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (YoPMetrics.class) {
        if (INSTANCE == null) {
          INSTANCE = DefaultMetricsSystem.instance().register(METRIC_NAME,
              "Metrics for the Yarn Router YoP Service", new YoPMetrics());
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  @VisibleForTesting
  public long getTotalConnectPelotonServices() {
    return totalSucceedConnectPelotonServices.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getTotalSucceedGetOrderedHosts() {
    return totalSucceedGetOrderedHostsFromRM.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getTotalSucceedReclaimHostOrder() {
    return totalSucceedReclaimHostOrder.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getTotalQueryNMJob() {
    return totalSucceedQueryNMJob.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumOfFailedGetOrderedHostsFromRM() {
    return numOfFailedGetOrderedHostsFromRM.value();
  }

  @VisibleForTesting
  public long getNumOfFailedSetReclaimHost() {
    return numOfFailedSetReclaimHost.value();
  }

  @VisibleForTesting
  public long getNumOfFailedFetchPelotonCreds() {
    return numOfFailedFetchPelotonCreds.value();
  }

  @VisibleForTesting
  public long getNumOfFailedQueryJob() {
    return numOfFailedQueryJob.value();
  }

  public void connectPelotonServices(long duration) {
    totalSucceedConnectPelotonServices.add(duration);
  }

  public void setSucceedGetOrderedHostsFromRM(long duration) {
    totalSucceedGetOrderedHostsFromRM.add(duration);
  }

  public void setSucceedReclaimHostOrder(long duration) {
    totalSucceedReclaimHostOrder.add(duration);
  }

  public void queryNMJob(long duration) {
    totalSucceedQueryNMJob.add(duration);
  }

  public void incrGetOrderedHostsFromRMFailure() {
    numOfFailedGetOrderedHostsFromRM.incr();
  }

  public void incrSetReclaimHostFailure() {
    numOfFailedSetReclaimHost.incr();
  }

  public void incrFetchPelotonCredstFailure() {
    numOfFailedFetchPelotonCreds.incr();
  }

  public void incrQueryJobFailure() {
    numOfFailedQueryJob.incr();
  }

}
