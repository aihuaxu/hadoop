package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class SavePelotonZKConfResponse {

  public static SavePelotonZKConfResponse newInstance() {
    return StateStoreSerializer.newRecord(SavePelotonZKConfResponse.class);
  }

  public static SavePelotonZKConfResponse newInstance(boolean status) {
    SavePelotonZKConfResponse response = newInstance();
    response.setStatus(status);
    return response;
  }

  public static SavePelotonZKConfResponse newInstance(boolean status, String errorMessage) {
    SavePelotonZKConfResponse response = newInstance();
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
