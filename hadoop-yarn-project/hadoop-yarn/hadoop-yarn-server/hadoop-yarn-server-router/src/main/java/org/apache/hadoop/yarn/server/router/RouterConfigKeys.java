package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.yarn.server.router.security.token.ZKDelegationTokenSecretManagerImpl;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;
import org.apache.hadoop.yarn.server.router.store.driver.impl.StateStoreSerializerPBImpl;
import org.apache.hadoop.yarn.server.router.store.driver.impl.StateStoreZooKeeperImpl;

import java.util.concurrent.TimeUnit;

public class RouterConfigKeys extends CommonConfigurationKeysPublic {
    public static final String ROUTER_PREFIX =
        "yarn.router.";
    private static final String ROUTER_STORE_PREFIX =
        ROUTER_PREFIX + "store.";
    public static final String ROUTER_SECRET_MANAGER_CLASS =
        ROUTER_PREFIX + "secret.manager.class";
    public static final Class<? extends AbstractDelegationTokenSecretManager>
        ROUTER_SECRET_MANAGER_CLASS_DEFAULT =
        ZKDelegationTokenSecretManagerImpl.class;

    public static final String ROUTER_CACHE_TIME_TO_LIVE_MS =
        ROUTER_PREFIX + "cache.ttl";
    public static final long ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT =
        TimeUnit.SECONDS.toMillis(3);

    public static final String ROUTER_STORE_DRIVER_CLASS =
        ROUTER_STORE_PREFIX + "driver.class";
    public static final Class<? extends StateStoreDriver>
        ROUTER_STORE_DRIVER_CLASS_DEFAULT = StateStoreZooKeeperImpl.class;

    public static final String ROUTER_STORE_ROUTER_EXPIRATION_MS =
        ROUTER_STORE_PREFIX + "router.expiration";
    public static final long ROUTER_STORE_ROUTER_EXPIRATION_MS_DEFAULT =
        TimeUnit.MINUTES.toMillis(5);

    public static final String ROUTER_STORE_SERIALIZER_CLASS =
        ROUTER_STORE_PREFIX + "serializer";
    public static final Class<? extends StateStoreSerializer>
        ROUTER_STORE_SERIALIZER_CLASS_DEFAULT =
        StateStoreSerializerPBImpl.class;
    public static final String ROUTER_HEARTBEAT_ENABLE =
        ROUTER_PREFIX + "heartbeat.enable";
    public static final boolean ROUTER_HEARTBEAT_ENABLE_DEFAULT = true;
    public static final String ROUTER_HEARTBEAT_STATE_INTERVAL_MS =
        ROUTER_PREFIX + "heartbeat-state.interval";
    public static final long ROUTER_HEARTBEAT_STATE_INTERVAL_MS_DEFAULT =
        TimeUnit.SECONDS.toMillis(5);

    public static final String ROUTER_SAFEMODE_ENABLE =
        ROUTER_PREFIX + "safemode.enable";
    public static final boolean ROUTER_SAFEMODE_ENABLE_DEFAULT = true;
    public static final String ROUTER_SAFEMODE_EXTENSION =
        ROUTER_PREFIX + "safemode.extension";
    public static final long ROUTER_SAFEMODE_EXTENSION_DEFAULT =
        TimeUnit.SECONDS.toMillis(30);
    public static final String ROUTER_SAFEMODE_EXPIRATION =
        ROUTER_PREFIX + "safemode.expiration";
    public static final long ROUTER_SAFEMODE_EXPIRATION_DEFAULT =
        3 * ROUTER_CACHE_TIME_TO_LIVE_MS_DEFAULT;

    public static final String ROUTER_STORE_ENABLE =
        ROUTER_PREFIX + "enable";
    public static final boolean ROUTER_STORE_ENABLE_DEFAULT = true;

    public static final String ROUTER_PELOTON_ZK_STORE_ENABLED =
        ROUTER_STORE_PREFIX + "peloton.zkconf.enabled";
    public static final boolean ROUTER_PELOTON_ZK_STORE_ENABLED_DEFAULT = true;

    // YARN Router admin
    public static final String ROUTER_ADMIN_HANDLER_COUNT_KEY =
        ROUTER_PREFIX + "admin.handler.count";
    public static final int ROUTER_ADMIN_HANDLER_COUNT_DEFAULT = 1;
    public static final int ROUTER_ADMIN_PORT_DEFAULT = 8111;
    public static final String ROUTER_ADMIN_ADDRESS_KEY =
        ROUTER_PREFIX + "admin-address";
    public static final String ROUTER_ADMIN_ADDRESS_DEFAULT =
        "0.0.0.0:" + ROUTER_ADMIN_PORT_DEFAULT;
    public static final String ROUTER_ADMIN_BIND_HOST_KEY =
        ROUTER_PREFIX + "admin-bind-host";
    public static final String ROUTER_ADMIN_ENABLE =
        ROUTER_PREFIX + "admin.enable";
    public static final boolean ROUTER_ADMIN_ENABLE_DEFAULT = true;
    public static final String
        SECURITY_ROUTER_ADMIN_SERVICE_PROTOCOL_ACL = "security.router.protocol.acl";
}
