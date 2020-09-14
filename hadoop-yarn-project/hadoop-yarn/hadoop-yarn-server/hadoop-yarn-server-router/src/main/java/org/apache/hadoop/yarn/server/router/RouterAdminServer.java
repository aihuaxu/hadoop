package org.apache.hadoop.yarn.server.router;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.proto.RouterAdministrationProtocol;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfManager;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
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
public class RouterAdminServer extends AbstractService implements PelotonZKConfManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAdminServer.class);

  private Configuration conf;

  private final Router router;

  private PelotonZKConfRecordStore pelotonZKConfRecordStore;

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
        true)) {
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

}
