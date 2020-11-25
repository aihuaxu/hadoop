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

package org.apache.hadoop.yarn.server.router;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.EmbeddedElector;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.apache.hadoop.yarn.server.router.store.RouterRecordStore;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebApp;
import org.apache.hadoop.yarn.server.router.external.peloton.YoPService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PORT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_HA_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_HA_ENABLED_DEFAULT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_KEYTAB_FILE_KEY;
import static org.apache.hadoop.yarn.server.router.RouterConfigKeys.ROUTER_HEARTBEAT_ENABLE;
import static org.apache.hadoop.yarn.server.router.RouterConfigKeys.ROUTER_HEARTBEAT_ENABLE_DEFAULT;
import static org.apache.hadoop.yarn.server.router.RouterConfigKeys.ROUTER_STORE_ENABLE;
import static org.apache.hadoop.yarn.server.router.RouterConfigKeys.ROUTER_STORE_ENABLE_DEFAULT;

import com.google.common.annotations.VisibleForTesting;

/**
 * The router is a stateless YARN component which is the entry point to the
 * cluster. It can be deployed on multiple nodes behind a Virtual IP (VIP) with
 * a LoadBalancer.
 *
 * The Router exposes the ApplicationClientProtocol (RPC and REST) to the
 * outside world, transparently hiding the presence of ResourceManager(s), which
 * allows users to request and update reservations, submit and kill
 * applications, and request status on running applications.
 *
 * In addition, it exposes the ResourceManager Admin API.
 *
 * This provides a placeholder for throttling mis-behaving clients (YARN-1546)
 * and masks the access to multiple RMs (YARN-3659).
 */
public class Router extends CompositeService {

  /** Router address/identifier. */
  private String routerId;
  private static final Logger LOG = LoggerFactory.getLogger(Router.class);
  private static CompositeServiceShutdownHook routerShutdownHook;
  private Configuration conf;
  private UserGroupInformation routerLoginUGI;
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private RouterClientRMService clientRMProxyService;
  private RouterRMAdminService rmAdminProxyService;
  private WebApp webApp;
  private RouterStateStoreService stateStoreService;
  /** Manages the current state of the router. */
  private RouterRecordStore routerRecordStore;
  /** Heartbeat our run status to the router state store. */
  private RouterHeartbeatService routerHeartbeatService;
  /** State of the Router. */
  private RouterServiceState routerServiceState = RouterServiceState.UNINITIALIZED;
  @VisibleForTesting
  protected String webAppAddress;
  private JvmMetrics jvmMetrics;

  /** Leader election. */
  private boolean routerHAEnabled = false;
  private RouterHAContext routerHAContext;
  private RouterActiveServices activeServices;
  private ZKCuratorManager zkManager;
  private final String zkRootNodePassword =
      Long.toString(new SecureRandom().nextLong());

  /** RPC interface for the admin. */
  private RouterAdminServer adminServer;
  private InetSocketAddress adminAddress;

  /**
   * Priority of the Router shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final String METRICS_NAME = "Router";

  public Router() {
    super(Router.class.getName());
  }

  protected void doSecureLogin() throws IOException {
    // Enable the security for the Router.
    // this change is backport from https://issues.apache.org/jira/browse/YARN-6539
    // since the JIRA is not fully merged into upstream, we copy the patch here
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, ROUTER_KEYTAB_FILE_KEY,
    ROUTER_KERBEROS_PRINCIPAL_KEY, getHostName(conf));

    // if security is enable, set routerLoginUGI as UGI of loginUser
    // getLoginUser function will call spawnAutoRenewalThreadForUserCreds which
    // handles the auto renew token.
    if (UserGroupInformation.isSecurityEnabled()) {
      this.routerLoginUGI = UserGroupInformation.getLoginUser();
    }
  }

  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;
    this.routerHAContext = new RouterHAContext();
    routerHAContext.setRouter(this);
    updateRouterState(RouterServiceState.INITIALIZING);
    String hostName = getHostName(conf);
    setRouterId(hostName + ":" + DEFAULT_ROUTER_CLIENTRM_PORT);
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed Router login", e);
    }
    // RouterStateStore Service
    if (conf.getBoolean(
        ROUTER_STORE_ENABLE,
        ROUTER_STORE_ENABLE_DEFAULT)) {
      // Service that maintains the State Store connection
      this.stateStoreService = new RouterStateStoreService();
      addService(this.stateStoreService);
    }
    // ClientRM Proxy
    clientRMProxyService = createClientRMProxyService();
    addService(clientRMProxyService);
    // RMAdmin Proxy
    rmAdminProxyService = createRMAdminProxyService();
    addService(rmAdminProxyService);
    // WebService
    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
        YarnConfiguration.ROUTER_BIND_HOST,
        WebAppUtils.getRouterWebAppURLWithoutScheme(this.conf));

    // Create admin server
    adminServer = createAdminServer();
    addService(adminServer);
    // add to routerHAContext for leader election
    routerHAContext.setRouterAdminServer(adminServer);

    // Metrics
    MetricsSystem ms = DefaultMetricsSystem.initialize(METRICS_NAME);
    jvmMetrics = JvmMetrics.initSingleton("Router", null);

    JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jvmMetrics.setPauseMonitor(pauseMonitor);

    // Router heartbeat service to periodically update the router status asynchronously in state store
    if (conf.getBoolean(
        ROUTER_HEARTBEAT_ENABLE,
        ROUTER_HEARTBEAT_ENABLE_DEFAULT)) {
      this.routerHeartbeatService = new RouterHeartbeatService(this);
      addService(this.routerHeartbeatService);
    }
    // Add leader election service and Router activities to be managed by leader
    routerHAEnabled = conf.getBoolean(ROUTER_HA_ENABLED, ROUTER_HA_ENABLED_DEFAULT);
    routerHAContext.setHAEnabled(routerHAEnabled);
    if (routerHAEnabled) {
      EmbeddedElector elector = createEmbeddedElector();
      addIfService(elector);
      routerHAContext.setLeaderElectorService(elector);
    }

    createAndInitActiveServices(conf, clientRMProxyService);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    startWepApp();
    super.serviceStart();
    // Router is running now
    updateRouterState(RouterServiceState.RUNNING);
  }

  @Override
  protected void serviceStop() throws Exception {
    // Update state
    updateRouterState(RouterServiceState.SHUTDOWN);
    if (webApp != null) {
      webApp.stop();
    }
    if (isStopping.getAndSet(true)) {
      return;
    }
    transitionToStandby();
    super.serviceStop();
    DefaultMetricsSystem.shutdown();
  }

  protected void shutDown() {
    new Thread() {
      @Override
      public void run() {
        Router.this.stop();
      }
    }.start();
  }

  protected RouterClientRMService createClientRMProxyService() {
    return new RouterClientRMService();
  }

  protected RouterRMAdminService createRMAdminProxyService() {
    return new RouterRMAdminService();
  }

  protected YoPService createYoPService(RouterClientRMService RouterClientRMService) {
    return new YoPService(stateStoreService, clientRMProxyService);
  }

  @Private
  public WebApp getWebapp() {
    return this.webApp;
  }

  @VisibleForTesting
  public void startWepApp() {

    RMWebAppUtil.setupSecurityAndFilters(conf, null);

    Builder<Object> builder =
        WebApps.$for("cluster", null, null, "ws").with(conf).at(webAppAddress);
    webApp = builder.start(new RouterWebApp(this));
  }

  /**
   * Update the router state and heartbeat to the state store.
   *
   * @param newState The new router state.
   */
  public void updateRouterState(RouterServiceState newState) {
    this.routerServiceState = newState;
    if (this.routerHeartbeatService != null) {
      this.routerHeartbeatService.updateStateAsync();
    }
  }

  /**
   * Unique ID for the router, typically the hostname:port string for the
   * router's RPC server. This ID may be null on router startup before the RPC
   * server has bound to a port.
   *
   * @return Router identifier.
   */
  public String getRouterId() {
    return this.routerId;
  }

  /**
   * Returns the hostname for this Router. If the hostname is not
   * explicitly configured in the given config, then it is determined.
   *
   * @param config configuration
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the hostname cannot be determined
   */
  private static String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY);
    if (name == null) {
      name = InetAddress.getLocalHost().getCanonicalHostName();
    }
    return name;
  }

  /**
   * Sets a unique ID for this router.
   *
   * @param id Identifier of the Router.
   */
  public void setRouterId(String id) {
    this.routerId = id;
    if (this.stateStoreService != null) {
      this.stateStoreService.setIdentifier(this.routerId);
    }
  }

  public RouterClientRMService getClientRMProxyService() {
    return this.clientRMProxyService;
  }

  /**
   * Get the State Store service.
   *
   * @return State Store service.
   */
  public RouterStateStoreService getStateStoreService() {
    return this.stateStoreService;
  }

  /**
   * Get the state store interface for the router heartbeats.
   *
   * @return
   */
  public RouterRecordStore getRouterRecordStore() {
    if (this.routerRecordStore == null && this.stateStoreService != null) {
      this.routerRecordStore = this.stateStoreService.getRegisteredRecordStore(
          RouterRecordStore.class);
    }
    return this.routerRecordStore;
  }

  /**
   * Set the current Admin socket for the router.
   *
   * @param address Admin RPC address.
   */
  protected void setAdminServerAddress(InetSocketAddress address) {
    this.adminAddress = address;
  }

  /**
   * Get the current Admin socket address for the router.
   *
   * @return InetSocketAddress Admin address.
   */
  public InetSocketAddress getAdminServerAddress() {
    return adminAddress;
  }

  /**
   * Get the status of the router.
   *
   * @return Status of the router.
   */
  public RouterServiceState getRouterState() {
    return this.routerServiceState;
  }

  /**
   * Create a new router admin server to handle the router admin interface.
   *
   * @return RouterAdminServer
   * @throws IOException If the admin server was not successfully started.
   */
  protected RouterAdminServer createAdminServer() throws IOException {
    return new RouterAdminServer(this.conf, this);
  }

  public static void main(String[] argv) {
    Configuration conf = new YarnConfiguration();
    Thread
        .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(Router.class, argv, LOG);
    Router router = new Router();
    try {

      // Remove the old hook if we are rebooting.
      if (null != routerShutdownHook) {
        ShutdownHookManager.get().removeShutdownHook(routerShutdownHook);
      }

      routerShutdownHook = new CompositeServiceShutdownHook(router);
      ShutdownHookManager.get().addShutdownHook(routerShutdownHook,
          SHUTDOWN_HOOK_PRIORITY);

      router.init(conf);
      router.start();
    } catch (Throwable t) {
      LOG.error("Error starting Router", t);
      System.exit(-1);
    }
  }

  /**
   * Leader election
   */
  protected EmbeddedElector createEmbeddedElector() throws IOException {
    EmbeddedElector elector;
    this.zkManager = createAndStartZKManager(conf);
    elector = new RouterElectorBasedElectorService(this, adminServer);
    return elector;
  }

  public CuratorFramework getCurator() {
    if (this.zkManager == null) {
      return null;
    }
    return this.zkManager.getCurator();
  }

  public RouterHAContext getRouterHAContext() {
    return this.routerHAContext;
  }

  synchronized void transitionToActive() throws Exception {
    if (routerHAContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
      LOG.info("Already in active state");
      return;
    }
    LOG.info("Transitioning to active state");
    this.routerLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          startActiveServices();
          return null;
        } catch (Exception e) {
          createAndInitActiveServices(conf, clientRMProxyService);
          throw e;
        }
      }
    });
    routerHAContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
    LOG.info("Transitioned to active state");
  }

  synchronized void transitionToStandby()
      throws Exception {
    if (routerHAContext.getHAServiceState() ==
        HAServiceProtocol.HAServiceState.STANDBY) {
      LOG.info("Already in standby state");
      return;
    }

    LOG.info("Transitioning to standby state");
    HAServiceProtocol.HAServiceState state = routerHAContext.getHAServiceState();
    routerHAContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
    if (state == HAServiceProtocol.HAServiceState.ACTIVE) {
      stopActiveServices();
      createAndInitActiveServices(conf, clientRMProxyService);
    }
    LOG.info("Transitioned to standby state");
  }

  private void createAndInitActiveServices(Configuration conf, RouterClientRMService clientRMService) {
    activeServices = new RouterActiveServices(clientRMService);
    activeServices.init(conf);
  }
  /**
   * Get ZooKeeper Curator manager, creating and starting if not exists.
   * @param config Configuration for the ZooKeeper curator.
   * @return ZooKeeper Curator manager.
   * @throws IOException If it cannot create the manager.
   */
  public ZKCuratorManager createAndStartZKManager(Configuration
      config) throws IOException {
    ZKCuratorManager manager = new ZKCuratorManager(config);
//    TODO: add fencing action
    manager.start();
    return manager;
  }

  /**
   * Helper method to start {@link #activeServices}.
   * @throws Exception
   */
  void startActiveServices() throws Exception {
    if (activeServices != null) {
      activeServices.start();
    }
  }

  /**
   * Helper method to stop {@link #activeServices}.
   * @throws Exception
   */
  void stopActiveServices() {
    if (activeServices != null) {
      activeServices.stop();
      activeServices = null;
    }
  }

  @Private
  public class RouterActiveServices extends CompositeService {
    private YoPService yopService;
    private RouterClientRMService clientRMService;

    RouterActiveServices(RouterClientRMService clientRMService) {
      super("RouterActiveServices");
      this.clientRMService = clientRMService;
    }

    @Override
    protected void serviceInit(Configuration configuration) throws Exception {
      if (conf.getBoolean(YarnConfiguration.PELOTON_SERVICE_ENABLED, YarnConfiguration.PELOTON_SERVICE_ENABLED_DEFAULT)) {
        LOG.info("Peloton service enabled.");
        yopService = createYoPService(clientRMService);
        yopService.init(conf);
        addService(yopService);
      }
    }
  }
}
