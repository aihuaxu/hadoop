package org.apache.hadoop.yarn.server.router.store.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.io.IOException;

/**
 * API response for registering a router with the state store.
 */
public abstract class RouterHeartbeatResponse {

  public static RouterHeartbeatResponse newInstance() throws IOException {
    return StateStoreSerializer.newRecord(RouterHeartbeatResponse.class);
  }

  public static RouterHeartbeatResponse newInstance(boolean status)
      throws IOException {
    RouterHeartbeatResponse response = newInstance();
    response.setStatus(status);
    return response;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract boolean getStatus();

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setStatus(boolean result);
}
