package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class ClearAllPelotonZKConfsRequest {

  public static ClearAllPelotonZKConfsRequest newInstance() {
    return StateStoreSerializer.newRecord(ClearAllPelotonZKConfsRequest.class);
  }
}
