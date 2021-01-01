package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.EmbeddedElector;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MockRouter extends Router{
  static final Logger LOG = Logger.getLogger(MockRouter.class);
  static final String ENABLE_WEBAPP = "mockrouter.webapp.enabled";
  private static final int SECOND = 1000;
  private static final int TIMEOUT_MS_FOR_ATTEMPT = 40 * SECOND;
  private static final int TIMEOUT_MS_FOR_APP_REMOVED = 40 * SECOND;
  private static final int TIMEOUT_MS_FOR_CONTAINER_AND_NODE = 20 * SECOND;
  private static final int WAIT_MS_PER_LOOP = 10;

  private final boolean useNullRMNodeLabelsManager;
  private boolean disableDrainEventsImplicitly;

  private boolean useRealElector = false;

  public MockRouter() {
    this(new YarnConfiguration());
  }

  public MockRouter(Configuration conf) {
    this(conf, null);
  }

  public MockRouter(Configuration conf, RouterStateStoreService store) {
    this(conf, store, true, false);
  }

  public MockRouter(Configuration conf, boolean useRealElector) {
    this(conf, null, true, useRealElector);
  }

  public MockRouter(Configuration conf, RouterStateStoreService store,
      boolean useRealElector) {
    this(conf, store, true, useRealElector);
  }

  public MockRouter(Configuration conf, RouterStateStoreService store,
      boolean useNullRMNodeLabelsManager, boolean useRealElector) {
    super();
    this.useNullRMNodeLabelsManager = useNullRMNodeLabelsManager;
    this.useRealElector = useRealElector;
    if (store != null) {

    } else {
      NullRouterStateStore mockStateStore = new NullRouterStateStore();
      mockStateStore.init(conf);
    }
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
  }

  @Override
  protected EmbeddedElector createEmbeddedElector() throws IOException {
    if (useRealElector) {
      return super.createEmbeddedElector();
    } else {
      return null;
    }
  }

}
