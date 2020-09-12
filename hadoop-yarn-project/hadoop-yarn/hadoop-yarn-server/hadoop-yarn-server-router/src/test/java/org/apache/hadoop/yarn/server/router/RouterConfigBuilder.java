package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.yarn.server.router.store.driver.impl.StateStoreZooKeeperImpl;

public class RouterConfigBuilder {
  private Configuration conf;
  private boolean enableHeartbeat = false;
  private boolean enableStateStore = false;

  public RouterConfigBuilder(Configuration configuration) {
    this.conf = configuration;
  }

  public RouterConfigBuilder() {
    this.conf = new Configuration(false);
  }

  public RouterConfigBuilder all() {
    this.enableHeartbeat = true;
    this.enableStateStore = true;
    return this;
  }

  public RouterConfigBuilder heartbeat(boolean enable) {
    this.enableHeartbeat = enable;
    return this;
  }

  public RouterConfigBuilder stateStore(boolean enable) {
    this.enableStateStore = enable;
    return this;
  }
  
  public RouterConfigBuilder heartbeat() {
    return this.heartbeat(true);
  }

  public RouterConfigBuilder stateStore() {
    // reset the State Store driver implementation class for testing
    conf.setClass(RouterConfigKeys.ROUTER_STORE_DRIVER_CLASS,
        StateStoreZooKeeperImpl.class,
        StateStoreDriver.class);
    return this.stateStore(true);
  }

  public Configuration build() {
    conf.setBoolean(RouterConfigKeys.ROUTER_STORE_ENABLE,
        this.enableStateStore);
    conf.setBoolean(RouterConfigKeys.ROUTER_HEARTBEAT_ENABLE,
        this.enableHeartbeat);
    return conf;
  }
}

