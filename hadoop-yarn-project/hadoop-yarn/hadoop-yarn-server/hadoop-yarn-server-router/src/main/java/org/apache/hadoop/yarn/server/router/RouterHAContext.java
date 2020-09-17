package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.EmbeddedElector;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.RouterAdminServer;

public class RouterHAContext {

  private boolean isHAEnabled;
  private HAServiceProtocol.HAServiceState haServiceState =
      HAServiceProtocol.HAServiceState.INITIALIZING;
  private RouterAdminServer adminServer;
  private EmbeddedElector elector;
  private Router router;
  private final Object haServiceStateLock = new Object();

  public Router getRouter() {
    return router;
  }
  public void setRouter(Router router) {
    this.router = router;
  }

  public EmbeddedElector getLeaderElectorService() {
    return this.elector;
  }

  public void setLeaderElectorService(EmbeddedElector embeddedElector) {
    this.elector = embeddedElector;
  }

  public RouterAdminServer getRouterAdminServer() {
    return this.adminServer;
  }

  void setRouterAdminServer(RouterAdminServer service) {
    this.adminServer = service;
  }

  void setHAEnabled(boolean routerHAEnabled) {
    this.isHAEnabled = routerHAEnabled;
  }

  public boolean isHAEnabled() {
    return isHAEnabled;
  }

  public HAServiceProtocol.HAServiceState getHAServiceState() {
    synchronized (haServiceStateLock) {
      return haServiceState;
    }
  }

  void setHAServiceState(HAServiceProtocol.HAServiceState serviceState) {
    synchronized (haServiceStateLock) {
      this.haServiceState = serviceState;
    }
  }

  public String getHAZookeeperConnectionState() {
    if (elector == null) {
      return "Could not find leader elector. Verify both HA and automatic "
          + "failover are enabled.";
    } else {
      return elector.getZookeeperConnectionState();
    }
  }

}