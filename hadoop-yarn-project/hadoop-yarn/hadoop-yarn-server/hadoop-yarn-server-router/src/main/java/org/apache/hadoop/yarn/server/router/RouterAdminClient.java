package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfManager;
import org.apache.hadoop.yarn.server.router.protocol.RouterAdminProtocolPB;
import org.apache.hadoop.yarn.server.router.protocol.RouterAdminProtocolTranslatorPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Client to connect to the {@link Router} via the admin protocol.
 */
@InterfaceAudience.Private
public class RouterAdminClient implements Closeable {

  private final RouterAdminProtocolTranslatorPB proxy;
  private final UserGroupInformation ugi;

  private static RouterAdminProtocolTranslatorPB
  createRouterProxy(InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
      throws IOException {
    RPC.setProtocolEngine(
        conf, RouterAdminProtocolPB.class, ProtobufRpcEngine.class);

    AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);
    final long version = RPC.getProtocolVersion(RouterAdminProtocolPB.class);
    RouterAdminProtocolPB proxy = RPC.getProtocolProxy(
        RouterAdminProtocolPB.class, version, address, ugi, conf,
        NetUtils.getDefaultSocketFactory(conf),
        RPC.getRpcTimeout(conf), null,
        fallbackToSimpleAuth).getProxy();



    return new RouterAdminProtocolTranslatorPB(proxy);
  }

  public RouterAdminClient(InetSocketAddress address, Configuration conf)
      throws IOException {
    this.ugi = UserGroupInformation.getLoginUser();
    this.proxy = createRouterProxy(address, conf, ugi);
  }

  public PelotonZKConfManager getPelotonZKConfManager() {
    return proxy;
  }


  @Override
  public synchronized void close() throws IOException {
    RPC.stopProxy(proxy);
  }
}
