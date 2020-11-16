package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class SavePelotonNodeLabelResponse {

  public static SavePelotonNodeLabelResponse newInstance() {
    return StateStoreSerializer.newRecord(SavePelotonNodeLabelResponse.class);
  }

  public static SavePelotonNodeLabelResponse newInstance(boolean status) {
    SavePelotonNodeLabelResponse response = newInstance();
    response.setStatus(status);
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
