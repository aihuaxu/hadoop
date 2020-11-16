package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class SavePelotonNodeLabelRequest {

  public static SavePelotonNodeLabelRequest newInstance() {
    return StateStoreSerializer.newRecord(SavePelotonNodeLabelRequest.class);
  }

  public static SavePelotonNodeLabelRequest newInstance(String label) {
    SavePelotonNodeLabelRequest request = newInstance();
    request.setNodeLabel(label);
    return request;
  }

  @InterfaceAudience.Public
  public abstract void setNodeLabel(String label);

  @InterfaceAudience.Public
  public abstract String getNodeLabel();
}
