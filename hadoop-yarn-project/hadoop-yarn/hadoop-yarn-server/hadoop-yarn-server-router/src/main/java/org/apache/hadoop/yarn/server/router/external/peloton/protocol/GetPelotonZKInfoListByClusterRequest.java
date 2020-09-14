package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class GetPelotonZKInfoListByClusterRequest {

  public static GetPelotonZKInfoListByClusterRequest newInstance() {
    return StateStoreSerializer.newRecord(GetPelotonZKInfoListByClusterRequest.class);
  }

  public static GetPelotonZKInfoListByClusterRequest newInstance(String cluster) {
    GetPelotonZKInfoListByClusterRequest request = newInstance();
    request.setCluster(cluster);
    return request;
  }

  @InterfaceAudience.Public
  public abstract void setCluster(String cluster);

  @InterfaceAudience.Public
  public abstract String getCluster();
}
