package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class RemovePelotonZKInfoFromClusterRequest {

  public static RemovePelotonZKInfoFromClusterRequest newInstance() {
    return StateStoreSerializer.newRecord(RemovePelotonZKInfoFromClusterRequest.class);
  }

  public static RemovePelotonZKInfoFromClusterRequest newInstance(String cluster, String zkAddress) {
    RemovePelotonZKInfoFromClusterRequest request = newInstance();
    request.setCluster(cluster);
    request.setZKAddress(zkAddress);
    return request;
  }

  @InterfaceAudience.Public
  public abstract void setCluster(String cluster);

  @InterfaceAudience.Public
  public abstract String getCluster();

  @InterfaceAudience.Public
  public abstract void setZKAddress(String zkAddress);

  @InterfaceAudience.Public
  public abstract String getZKAddress();

}
