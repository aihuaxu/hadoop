package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.router.security.token.ZKDelegationTokenSecretManagerImpl;

public class RouterConfigKeys extends CommonConfigurationKeysPublic {
    public static final String ROUTER_PREFIX =
            "yarn.router.";
    public static final String ROUTER_SECRET_MANAGER_CLASS =
            ROUTER_PREFIX + "secret.manager.class";
    public static final Class<? extends AbstractDelegationTokenSecretManager>
            ROUTER_SECRET_MANAGER_CLASS_DEFAULT =
            ZKDelegationTokenSecretManagerImpl.class;
}
