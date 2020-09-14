package org.apache.hadoop.yarn.server.router.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.yarn.proto.RouterAdministrationProtocol.RouterAdministrationProtocolService;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_KEY;

/**
 * Protocol that a clients use to communicate with the RouterAdmin.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@KerberosInfo(
    serverPrincipal = ROUTER_KERBEROS_PRINCIPAL_KEY)
@ProtocolInfo(
    protocolName = "org.apache.hadoop.yarn.server.router.protocolPB.RouterAdminProtocolPB",
    protocolVersion = 1)
public interface RouterAdminProtocolPB extends
    RouterAdministrationProtocolService.BlockingInterface {
}