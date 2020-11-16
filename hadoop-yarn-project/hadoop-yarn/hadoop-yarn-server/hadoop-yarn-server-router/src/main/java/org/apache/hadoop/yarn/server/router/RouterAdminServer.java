package org.apache.hadoop.yarn.server.router;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.proto.RouterAdministrationProtocol;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonNodeLabelManager;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonNodeLabelRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfManager;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.protocol.RouterAdminProtocolPB;
import org.apache.hadoop.yarn.server.router.protocol.RouterAdminProtocolServerSideTranslatorPB;
import org.apache.hadoop.yarn.server.router.security.authorize.RouterAdminPolicyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This class is responsible for handling all of the Admin calls to the YARN
 * router. It is created, started, and stopped by {@link Router}.
 */
public class RouterAdminServer extends AbstractService implements HAServiceProtocol, PelotonZKConfManager, PelotonNodeLabelManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAdminServer.class);

  private Configuration conf;

  private final Router router;

  private PelotonZKConfRecordStore pelotonZKConfRecordStore;
  private PelotonNodeLabelRecordStore pelotonNodeLabelRecordStore;

  /** The Admin server that listens to requests from clients. */
  private RPC.Server adminServer;
  private InetSocketAddress adminAddress;

  public RouterAdminServer(Configuration conf, Router router)
      throws IOException {
    super(RouterAdminServer.class.getName());

    this.conf = conf;
    this.router = router;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting RouterAdminServer");
    int handlerCount = this.conf.getInt(
        RouterConfigKeys.ROUTER_ADMIN_HANDLER_COUNT_KEY,
        RouterConfigKeys.ROUTER_ADMIN_HANDLER_COUNT_DEFAULT);

    RPC.setProtocolEngine(this.conf, RouterAdminProtocolPB.class,
        ProtobufRpcEngine.class);

    RouterAdminProtocolServerSideTranslatorPB routerAdminProtocolTranslator =
        new RouterAdminProtocolServerSideTranslatorPB(this);
    BlockingService clientRouterAdminPbService = RouterAdministrationProtocol.RouterAdministrationProtocolService.
        newReflectiveBlockingService(routerAdminProtocolTranslator);

    InetSocketAddress confRpcAddress = conf.getSocketAddr(
        RouterConfigKeys.ROUTER_ADMIN_BIND_HOST_KEY,
        RouterConfigKeys.ROUTER_ADMIN_ADDRESS_KEY,
        RouterConfigKeys.ROUTER_ADMIN_ADDRESS_DEFAULT,
        RouterConfigKeys.ROUTER_ADMIN_PORT_DEFAULT);

    String bindHost = conf.get(
        RouterConfigKeys.ROUTER_ADMIN_BIND_HOST_KEY,
        confRpcAddress.getHostName());
    LOG.info("Admin server binding to {}:{}",
        bindHost, confRpcAddress.getPort());

    this.adminServer = new RPC.Builder(this.conf)
        .setProtocol(RouterAdminProtocolPB.class)
        .setInstance(clientRouterAdminPbService)
        .setBindAddress(bindHost)
        .setPort(confRpcAddress.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false)) {
      adminServer.refreshServiceAclWithLoadedConfiguration(conf, RouterAdminPolicyProvider.getInstance());
    }

    InetSocketAddress listenAddress = this.adminServer.getListenerAddress();
    this.adminAddress = new InetSocketAddress(
        confRpcAddress.getHostName(), listenAddress.getPort());
    router.setAdminServerAddress(this.adminAddress);

    this.adminServer.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.adminServer != null) {
      this.adminServer.stop();
    }
    super.serviceStop();
  }

  /**
   * Get the RPC address of the admin service.
   * @return Administration service RPC address.
   */
  public InetSocketAddress getRpcAddress() {
    return this.adminAddress;
  }

  @Override
  public GetPelotonZKConfListResponse getPelotonZKConfList(
      GetPelotonZKConfListRequest request) throws IOException {
    return getPelotonZKConfRecordStore().getPelotonZKConfList(request);
  }

  @Override
  public SavePelotonZKConfResponse savePelotonZKConf(
      SavePelotonZKConfRequest request) throws IOException {
    return getPelotonZKConfRecordStore().savePelotonZKConf(request);
  }

  @Override
  public RemovePelotonZKConfByClusterResponse removePelotonZKConfByCluster(
      RemovePelotonZKConfByClusterRequest request) throws IOException {
    return getPelotonZKConfRecordStore().removePelotonZKConfByCluster(request);
  }

  @Override
  public ClearAllPelotonZKConfsResponse clearAllPelotonZKConfs(
      ClearAllPelotonZKConfsRequest request) throws IOException {
    return getPelotonZKConfRecordStore().clearAllPelotonZKConfs(request);
  }

  @Override
  public GetPelotonZKInfoListByClusterResponse getPelotonZKConfListByCluster(
      GetPelotonZKInfoListByClusterRequest request) throws IOException {
    return getPelotonZKConfRecordStore().getPelotonZKConfListByCluster(request);
  }

  @Override
  public SavePelotonZKInfoToClusterResponse savePelotonZKInfoToCluster(
      SavePelotonZKInfoToClusterRequest request) throws IOException {
    return getPelotonZKConfRecordStore().savePelotonZKInfoToCluster(request);
  }

  @Override
  public RemovePelotonZKInfoFromClusterResponse removePelotonZKInfoFromCluster(
      RemovePelotonZKInfoFromClusterRequest request) throws IOException {
    return getPelotonZKConfRecordStore().removePelotonZKInfoFromCluster(request);
  }

  @Override
  public GetPelotonNodeLabelResponse getPelotonNodeLabel(GetPelotonNodeLabelRequest request) throws IOException {
    return getPelotonNodeLabelRecordStore().getPelotonNodeLabel(request);
  }

  @Override
  public SavePelotonNodeLabelResponse savePelotonNodeLabel(SavePelotonNodeLabelRequest request) throws IOException {
    return getPelotonNodeLabelRecordStore().savePelotonNodeLabel(request);
  }

  private PelotonNodeLabelRecordStore getPelotonNodeLabelRecordStore() throws IOException {
    if (this.pelotonNodeLabelRecordStore == null) {
      this.pelotonNodeLabelRecordStore = router.getStateStoreService().getRegisteredRecordStore(
          PelotonNodeLabelRecordStore.class);
      if (this.pelotonNodeLabelRecordStore == null) {
        throw new IOException("pelotonNodeLabelRecordStore is not available.");
      }
    }
    return this.pelotonNodeLabelRecordStore;
  }

  private PelotonZKConfRecordStore getPelotonZKConfRecordStore() throws IOException {
    if (this.pelotonZKConfRecordStore == null) {
      this.pelotonZKConfRecordStore = router.getStateStoreService().getRegisteredRecordStore(
          PelotonZKConfRecordStore.class);
      if (this.pelotonZKConfRecordStore == null) {
        throw new IOException("pelotonZKConfRecordStore is not available.");
      }
    }
    return this.pelotonZKConfRecordStore;
  }

  @Override
  public synchronized void monitorHealth() throws HealthCheckFailedException, AccessControlException, IOException {
    if(isRouterActive()) {
      throw new HealthCheckFailedException(
          "Active Router services are not running!");
    }
  }

  @Override
  public synchronized void transitionToActive(
      StateChangeRequestInfo reqInfo)
      throws ServiceFailedException, AccessControlException, IOException {
    if (isRouterActive()) {
      return;
    }
    try {
      router.transitionToActive();
    } catch (Exception e) {
      throw new ServiceFailedException("Can not active", e);
    }
  }

  @Override
  public synchronized void transitionToStandby(StateChangeRequestInfo reqInfo)
      throws ServiceFailedException, AccessControlException, IOException {
    try {
      router.transitionToStandby();
    } catch (Exception e) {
      throw new ServiceFailedException("Can not standby", e);
    }
  }

  @Override
  public HAServiceStatus getServiceStatus() throws AccessControlException, IOException {
    HAServiceState haState = router.getRouterHAContext().getHAServiceState();
    HAServiceStatus ret = new HAServiceStatus(haState);
    if (isRouterActive() || haState == HAServiceState.STANDBY) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + haState);
    }
    return ret;
  }

  private synchronized boolean isRouterActive() {
    return HAServiceState.ACTIVE == router.getRouterHAContext().getHAServiceState();
  }
}
