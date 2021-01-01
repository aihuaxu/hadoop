package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;

public class NullRouterStateStore extends RouterStateStoreService {
  @Override
  protected void serviceInit(Configuration config) throws Exception {
//    Do nothing
  }
  @Override
  protected void serviceStart() throws Exception {
//    Do nothing
  }
  @Override
  protected void serviceStop() throws Exception {
//    Do nothing
  }

  @Override
  public StateStoreDriver getDriver() {
    return null;
  }
  @Override
  public boolean isDriverReady() {
    return true;
  }
  @Override
  public void refreshCaches(boolean force) {
//    Do nothing
  }
  @Override
  public void loadDriver() {
//    Do nothing
  }
}
