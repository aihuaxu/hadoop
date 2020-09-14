package org.apache.hadoop.yarn.server.router;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestRouter {
    private static Configuration conf;

    @BeforeClass
    public static void create() {
        // Basic configuration without the state store
        conf = new Configuration();
        conf.set(YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_KEY, "user@_HOST/REALM");
        conf.set(YarnConfiguration.ROUTER_KEYTAB_FILE_KEY, "/testfile");

        conf.set(YarnConfiguration.ROUTER_BIND_HOST, "0.0.0.0");
        conf.set(YarnConfiguration.ROUTER_CLIENTRM_ADDRESS, "testhost.com");
        conf.set(YarnConfiguration.ROUTER_RMADMIN_ADDRESS, "testhostadmin.com");
    }

    private static void testRouterStartup(Configuration routerConfig)
            throws InterruptedException, IOException {
        Router router = new Router();
        assertEquals(Service.STATE.NOTINITED, router.getServiceState());
        router.init(routerConfig);
        assertEquals(Service.STATE.INITED, router.getServiceState());
        router.start();
        assertEquals(Service.STATE.STARTED, router.getServiceState());
        router.stop();
        assertEquals(Service.STATE.STOPPED, router.getServiceState());
        router.close();
    }

    @Test
    public void testRouterService() throws InterruptedException, IOException {
        testRouterStartup(conf);
        // Admin only
        testRouterStartup(new RouterConfigBuilder(conf).build());
        // Statestore only
        testRouterStartup(new RouterConfigBuilder(conf).stateStore().build());
        // Heartbeat only
        testRouterStartup(new RouterConfigBuilder(conf).heartbeat().build());
    }
}
