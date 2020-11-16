package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class GetPelotonNodeLabelResponse {

  public static GetPelotonNodeLabelResponse newInstance() {
    return StateStoreSerializer.newRecord(GetPelotonNodeLabelResponse.class);
  }

  @InterfaceAudience.Public
  public abstract String getPelotonNodeLabel();

  @InterfaceAudience.Public
  public abstract void setPelotonNodeLabel(String nodeLabel);
}
