package org.apache.hadoop.yarn.server.router;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.EmbeddedElector;

import java.io.IOException;

/**
 * Router Leader election implementation using Curator with Leader Latch.
 * use EmbeddedElector interface from RM
 */
public class RouterElectorBasedElectorService extends AbstractService implements EmbeddedElector,
    LeaderLatchListener {

  public static final Log LOG =
      LogFactory.getLog(RouterElectorBasedElectorService.class);
  private LeaderLatch leaderLatch;
  private CuratorFramework curator;
  private String latchPath;
  private String routerId;
  private Router router;
  private RouterAdminServer adminServer;

  public RouterElectorBasedElectorService(Router router, RouterAdminServer adminServer) {
    super(RouterElectorBasedElectorService.class.getName());
    this.router = router;
    this.adminServer = adminServer;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    routerId = HAUtil.getRouterHAId(conf);
    String clusterId = YarnConfiguration.getClusterId(conf);
    String zkBasePath = conf.get(
        YarnConfiguration.ROUTER_AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.ROUTER_DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    latchPath = zkBasePath + "/" + clusterId;
    curator = router.getCurator();
    super.serviceInit(conf);
  }

  private void initAndStartLeaderLatch() throws Exception {
    leaderLatch = new LeaderLatch(curator, latchPath, routerId);
    leaderLatch.addListener(this);
    leaderLatch.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    initAndStartLeaderLatch();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeLeaderLatch();
    super.serviceStop();
  }

  @Override
  public void rejoinElection() {
    try {
      closeLeaderLatch();
      Thread.sleep(1000);
      initAndStartLeaderLatch();
    } catch (Exception e) {
      LOG.info("Fail to re-join election.", e);
    }
  }

  @Override
  public String getZookeeperConnectionState() {
    return "Connected to zookeeper : " +
        curator.getZookeeperClient().isConnected();
  }

  @Override
  public void isLeader() {
    LOG.info(routerId + " is elected leader, transitioning to active");
    try {
      adminServer.transitionToActive(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(routerId + " failed to transition to active, giving up leadership",
          e);
      notLeader();
      rejoinElection();
    }
  }

  private void closeLeaderLatch() throws IOException {
    if (leaderLatch != null) {
      leaderLatch.close();
    }
  }

  @Override
  public void notLeader() {
    LOG.info(routerId + " relinquish leadership");
    try {
      adminServer.transitionToStandby(
          new HAServiceProtocol.StateChangeRequestInfo(
              HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
    } catch (Exception e) {
      LOG.info(routerId + " did not transition to standby successfully.");
    }
  }

  // only for testing
  @VisibleForTesting
  public CuratorFramework getCuratorClient() {
    return this.curator;
  }

}