package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class GetPelotonZKConfListRequest {

  public static GetPelotonZKConfListRequest newInstance() {
    return StateStoreSerializer.newRecord(GetPelotonZKConfListRequest.class);
  }
}
