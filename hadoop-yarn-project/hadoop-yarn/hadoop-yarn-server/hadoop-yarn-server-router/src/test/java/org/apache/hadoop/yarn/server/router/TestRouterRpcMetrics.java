package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.server.router.metrics.RouterRpcMetrics;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRouterRpcMetrics {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestRouterRpcMetrics.class);

  private MockHappySenarios happySenarios = new MockHappySenarios();
  private MockUnhappySenarios unHappySenarios = new MockUnhappySenarios();
  private static RouterRpcMetrics metrics = RouterRpcMetrics.getMetrics();

  @BeforeClass
  public static void init() {

    LOG.info("Test: aggregate metrics are initialized correctly");

    Assert.assertEquals(0, metrics.getNumSucceededAppsCreated());
    Assert.assertEquals(0, metrics.getNumSucceededAppsSubmitted());
    Assert.assertEquals(0, metrics.getNumSucceededAppsKilled());
    Assert.assertEquals(0, metrics.getNumSucceededAppsRetrieved());

    Assert.assertEquals(0, metrics.getAppsFailedCreated());
    Assert.assertEquals(0, metrics.getAppsFailedSubmitted());
    Assert.assertEquals(0, metrics.getAppsFailedKilled());
    Assert.assertEquals(0, metrics.getAppsFailedRetrieved());

    LOG.info("Test: aggregate metrics are updated correctly");
  }

  /**
   * This test validates the correctness of the metric: Created Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsCreated() {

    long totalBefore = metrics.getNumSucceededAppsCreated();
    happySenarios.getNewApplication(100);
    Assert.assertEquals(totalBefore + 1,
        metrics.getNumSucceededAppsCreated());
    Assert.assertEquals(100, metrics.getLatencySucceededAppsCreated(), 0);
    happySenarios.getNewApplication(200);
    Assert.assertEquals(totalBefore + 2,
        metrics.getNumSucceededAppsCreated());
    Assert.assertEquals(150, metrics.getLatencySucceededAppsCreated(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to create Apps.
   */
  @Test
  public void testAppsFailedCreated() {

    long totalBadbefore = metrics.getAppsFailedCreated();

    unHappySenarios.getNewApplication();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedCreated());
  }

  /**
   * This test validates the correctness of the metric: Submitted Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsSubmitted() {

    long totalGoodBefore = metrics.getNumSucceededAppsSubmitted();

    happySenarios.submitApplication(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsSubmitted());
    Assert.assertEquals(100, metrics.getLatencySucceededAppsSubmitted(), 0);

    happySenarios.submitApplication(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsSubmitted());
    Assert.assertEquals(150, metrics.getLatencySucceededAppsSubmitted(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to submit Apps.
   */
  @Test
  public void testAppsFailedSubmitted() {

    long totalBadbefore = metrics.getAppsFailedSubmitted();

    unHappySenarios.submitApplication();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedSubmitted());
  }

  /**
   * This test validates the correctness of the metric: Killed Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsKilled() {

    long totalGoodBefore = metrics.getNumSucceededAppsKilled();

    happySenarios.forceKillApplication(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsKilled());
    Assert.assertEquals(100, metrics.getLatencySucceededAppsKilled(), 0);

    happySenarios.forceKillApplication(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsKilled());
    Assert.assertEquals(150, metrics.getLatencySucceededAppsKilled(), 0);
  }

  /**
   * This test validates the correctness of the metric: Retrieved Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsReport() {

    long totalGoodBefore = metrics.getNumSucceededAppsRetrieved();

    happySenarios.getApplicationReport(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsRetrieved());
    Assert.assertEquals(100, metrics.getLatencySucceededGetAppReport(), 0);

    happySenarios.getApplicationReport(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededGetAppReport(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to retrieve Apps.
   */
  @Test
  public void testAppsReportFailed() {

    long totalBadbefore = metrics.getAppsFailedRetrieved();

    unHappySenarios.getApplicationReport();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedRetrieved());
  }

  /**
   * This test validates the correctness of the metric: Retrieved Multiple Apps
   * successfully.
   */
  @Test
  public void testSucceededMultipleAppsReport() {

    long totalGoodBefore = metrics.getNumSucceededMultipleAppsRetrieved();

    happySenarios.getApplicationsReport(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededMultipleAppsRetrieved());
    Assert.assertEquals(100, metrics.getLatencySucceededMultipleGetAppReport(),
        0);

    happySenarios.getApplicationsReport(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededMultipleAppsRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededMultipleGetAppReport(),
        0);
  }

  /**
   * This test validates the correctness of the metric: Failed to retrieve
   * Multiple Apps.
   */
  @Test
  public void testMulipleAppsReportFailed() {

    long totalBadbefore = metrics.getMultipleAppsFailedRetrieved();

    unHappySenarios.getApplicationsReport();

    Assert.assertEquals(totalBadbefore + 1,
        metrics.getMultipleAppsFailedRetrieved());
  }

  // Records successes for all calls
  private class MockHappySenarios {
    public void getNewApplication(long duration) {
      LOG.info("Mocked: successful getNewApplication call with duration {}",
          duration);
      metrics.succeededAppsCreated(duration);
    }

    public void submitApplication(long duration) {
      LOG.info("Mocked: successful submitApplication call with duration {}",
          duration);
      metrics.succeededAppsSubmitted(duration);
    }

    public void forceKillApplication(long duration) {
      LOG.info("Mocked: successful forceKillApplication call with duration {}",
          duration);
      metrics.succeededAppsKilled(duration);
    }

    public void getApplicationReport(long duration) {
      LOG.info("Mocked: successful getApplicationReport call with duration {}",
          duration);
      metrics.succeededAppsRetrieved(duration);
    }

    public void getApplicationsReport(long duration) {
      LOG.info("Mocked: successful getApplicationsReport call with duration {}",
          duration);
      metrics.succeededMultipleAppsRetrieved(duration);
    }
  }

  // Records failures for all calls
  private class MockUnhappySenarios {
    public void getNewApplication() {
      LOG.info("Mocked: failed getNewApplication call");
      metrics.incrAppsFailedCreated();
    }

    public void submitApplication() {
      LOG.info("Mocked: failed submitApplication call");
      metrics.incrAppsFailedSubmitted();
    }

    public void forceKillApplication() {
      LOG.info("Mocked: failed forceKillApplication call");
      metrics.incrAppsFailedKilled();
    }

    public void getApplicationReport() {
      LOG.info("Mocked: failed getApplicationReport call");
      metrics.incrAppsFailedRetrieved();
    }

    public void getApplicationsReport() {
      LOG.info("Mocked: failed getApplicationsReport call");
      metrics.incrMultipleAppsFailedRetrieved();
    }
  }
}
