package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class RemovePelotonZKInfoFromClusterResponse {

  public static RemovePelotonZKInfoFromClusterResponse newInstance() {
    return StateStoreSerializer.newRecord(RemovePelotonZKInfoFromClusterResponse.class);
  }

  public static RemovePelotonZKInfoFromClusterResponse newInstance(boolean status) {
    RemovePelotonZKInfoFromClusterResponse response = newInstance();
    response.setStatus(status);
    return response;
  }

  public static RemovePelotonZKInfoFromClusterResponse newInstance(boolean status, String errorMessage) {
    RemovePelotonZKInfoFromClusterResponse response = newInstance();
    response.setStatus(status);
    response.setErrorMessage(errorMessage);
    return response;
  }

  @InterfaceAudience.Public
  public abstract void setStatus(boolean result);

  @InterfaceAudience.Public
  public abstract boolean getStatus();

  @InterfaceAudience.Public
  public abstract void setErrorMessage(String errorMessage);

  @InterfaceAudience.Public
  public abstract String getErrorMessage();
}
