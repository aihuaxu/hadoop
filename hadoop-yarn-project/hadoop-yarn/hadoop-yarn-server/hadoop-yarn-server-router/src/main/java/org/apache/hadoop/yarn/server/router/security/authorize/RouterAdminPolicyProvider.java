package org.apache.hadoop.yarn.server.router.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.apache.hadoop.yarn.server.router.protocol.RouterAdminProtocolPB;

public class RouterAdminPolicyProvider extends PolicyProvider {

  private static RouterAdminPolicyProvider routerAdminPolicyProvider = null;

  private RouterAdminPolicyProvider() {}

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static RouterAdminPolicyProvider getInstance() {
    if (routerAdminPolicyProvider == null) {
      synchronized(RouterAdminPolicyProvider.class) {
        if (routerAdminPolicyProvider == null) {
          routerAdminPolicyProvider = new RouterAdminPolicyProvider();
        }
      }
    }
    return routerAdminPolicyProvider;
  }

  private static final Service[] routerAdminServices =
      new Service[] {
          new Service(
              RouterConfigKeys.SECURITY_ROUTER_ADMIN_SERVICE_PROTOCOL_ACL,
              RouterAdminProtocolPB.class)
      };
  @Override
  public Service[] getServices() {
    return routerAdminServices;
  }
}

