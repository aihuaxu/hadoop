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
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.apache.hadoop.yarn.server.router.store.RouterRecordStore;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebApp;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PORT;
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
    ROUTER_KERBEROS_PRINCIPAL_KEY);
  }

  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;
    updateRouterState(RouterServiceState.INITIALIZING);
    String hostName = getHostName(conf);
    setRouterId(hostName + ":" + DEFAULT_ROUTER_CLIENTRM_PORT);
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
    // Metrics
    DefaultMetricsSystem.initialize(METRICS_NAME);

    // Router heartbeat service to periodically update the router status asynchronously in state store
    if (conf.getBoolean(
        ROUTER_HEARTBEAT_ENABLE,
        ROUTER_HEARTBEAT_ENABLE_DEFAULT)) {
      this.routerHeartbeatService = new RouterHeartbeatService(this);
      addService(this.routerHeartbeatService);
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed Router login", e);
    }
    startWepApp();
    // Router is running now
    updateRouterState(RouterServiceState.RUNNING);
    super.serviceStart();
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
   * Get the status of the router.
   *
   * @return Status of the router.
   */
  public RouterServiceState getRouterState() {
    return this.routerServiceState;
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
}
