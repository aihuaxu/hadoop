package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class RemovePelotonZKConfByClusterRequest {

  public static RemovePelotonZKConfByClusterRequest newInstance() {
    return StateStoreSerializer.newRecord(RemovePelotonZKConfByClusterRequest.class);
  }

  public static RemovePelotonZKConfByClusterRequest newInstance(String cluster) {
    RemovePelotonZKConfByClusterRequest request = newInstance();
    request.setCluster(cluster);
    return request;
  }

  @InterfaceAudience.Public
  public abstract void setCluster(String cluster);
  @InterfaceAudience.Public
  public abstract String getCluster();
}
