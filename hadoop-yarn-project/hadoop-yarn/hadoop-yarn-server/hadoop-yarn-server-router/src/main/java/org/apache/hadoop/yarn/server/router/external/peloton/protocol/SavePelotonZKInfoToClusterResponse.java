package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class SavePelotonZKInfoToClusterResponse {

  public static SavePelotonZKInfoToClusterResponse newInstance() {
    return StateStoreSerializer.newRecord(SavePelotonZKInfoToClusterResponse.class);
  }

  public static SavePelotonZKInfoToClusterResponse newInstance(boolean status) {
    SavePelotonZKInfoToClusterResponse response = newInstance();
    response.setStatus(status);
    return response;
  }

  public static SavePelotonZKInfoToClusterResponse newInstance(boolean status, String errorMsg) {
    SavePelotonZKInfoToClusterResponse response = newInstance();
    response.setStatus(status);
    response.setErrorMessage(errorMsg);
    return response;
  }
  @InterfaceAudience.Public
  public abstract boolean getStatus();

  @InterfaceAudience.Public
  public abstract void setStatus(boolean result);

  @InterfaceAudience.Public
  public abstract void setErrorMessage(String errorMessage);

  @InterfaceAudience.Public
  public abstract String getErrorMessage();
}
