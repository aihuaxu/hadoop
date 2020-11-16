package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class GetPelotonNodeLabelRequest {

  public static GetPelotonNodeLabelRequest newInstance() {
    return StateStoreSerializer.newRecord(GetPelotonNodeLabelRequest.class);
  }
}
