package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class ClearAllPelotonZKConfsResponse {

  public static ClearAllPelotonZKConfsResponse newInstance() {
    return StateStoreSerializer.newRecord(ClearAllPelotonZKConfsResponse.class);
  }

  public static ClearAllPelotonZKConfsResponse newInstance(boolean status) {
    ClearAllPelotonZKConfsResponse response = newInstance();
    response.setStatus(status);
    return response;
  }

  public static ClearAllPelotonZKConfsResponse newInstance(boolean status, String errorMessage) {
    ClearAllPelotonZKConfsResponse response = newInstance();
    response.setStatus(status);
    response.setErrorMessage(errorMessage);
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
