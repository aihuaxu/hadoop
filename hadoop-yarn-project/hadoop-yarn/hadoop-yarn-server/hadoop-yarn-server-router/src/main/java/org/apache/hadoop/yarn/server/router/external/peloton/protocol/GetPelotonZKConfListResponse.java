package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.util.List;

public abstract class GetPelotonZKConfListResponse {

  public static GetPelotonZKConfListResponse newInstance() {
    return StateStoreSerializer.newRecord(GetPelotonZKConfListResponse.class);
  }

  @InterfaceAudience.Public
  public abstract List<PelotonZKConf> getPelotonZKConfList();

  @InterfaceAudience.Public
  public abstract void setPelotonZKConfList(List<PelotonZKConf> zkConfs);
}
