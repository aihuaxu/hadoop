package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class SavePelotonZKInfoToClusterRequest {
  public static SavePelotonZKInfoToClusterRequest newInstance() {
    return StateStoreSerializer.newRecord(SavePelotonZKInfoToClusterRequest.class);
  }

  public static SavePelotonZKInfoToClusterRequest newInstance(String cluster, PelotonZKInfo zkInfo, boolean isCreate) {
    SavePelotonZKInfoToClusterRequest request = newInstance();
    request.setCluster(cluster);
    request.setPelotonInfo(zkInfo);
    request.setIsCreateNew(isCreate);
    return request;
  }

  @InterfaceAudience.Public
  public abstract void setCluster(String cluster);

  @InterfaceAudience.Public
  public abstract void setPelotonInfo(PelotonZKInfo zkInfo);

  @InterfaceAudience.Public
  public abstract String getCluster();

  @InterfaceAudience.Public
  public abstract PelotonZKInfo getPelotonZkInfo();

  @InterfaceAudience.Public
  public abstract void setIsCreateNew(boolean isCreate);

  @InterfaceAudience.Public
  public abstract boolean getIsCreate();
}
