package org.apache.hadoop.yarn.server.router.store.protocol;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;

import java.io.IOException;

/**
 * API request for registering a router with the state store.
 */
public abstract class RouterHeartbeatRequest {
  public static RouterHeartbeatRequest newInstance() throws IOException {
    return StateStoreSerializer.newRecord(RouterHeartbeatRequest.class);
  }

  public static RouterHeartbeatRequest newInstance(RouterState router)
      throws IOException {
    RouterHeartbeatRequest request = newInstance();
    request.setRouter(router);
    return request;
  }

  @Public
  public abstract RouterState getRouter() throws IOException;

  @Public
  public abstract void setRouter(RouterState routerState);
}
