package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.util.List;

public abstract class GetPelotonZKInfoListByClusterResponse {

  public static GetPelotonZKInfoListByClusterResponse newInstance() {
    return StateStoreSerializer.newRecord(GetPelotonZKInfoListByClusterResponse.class);
  }

  @InterfaceAudience.Public
  public abstract List<PelotonZKInfo> getPelotonZKInfoList();

  @InterfaceAudience.Public
  public abstract void setPelotonZKInfoList(List<PelotonZKInfo> zkInfoList);
}
