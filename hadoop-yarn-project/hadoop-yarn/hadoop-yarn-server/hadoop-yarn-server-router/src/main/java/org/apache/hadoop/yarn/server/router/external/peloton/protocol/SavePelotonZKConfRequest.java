package org.apache.hadoop.yarn.server.router.external.peloton.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

public abstract class SavePelotonZKConfRequest {
  public static SavePelotonZKConfRequest newInstance() {
    return StateStoreSerializer.newRecord(SavePelotonZKConfRequest.class);
  }

  public static SavePelotonZKConfRequest newInstance(PelotonZKConf zkConf, boolean isCreate) {
    SavePelotonZKConfRequest request = newInstance();
    request.setPelotonZKConf(zkConf);
    request.setIsCreate(isCreate);
    return request;
  }

  @InterfaceAudience.Public
  public abstract void setPelotonZKConf(PelotonZKConf zkConf);
  @InterfaceAudience.Public
  public abstract PelotonZKConf getPelotonZKConf();
  @InterfaceAudience.Public
  public abstract void setIsCreate(boolean isCreate);
  @InterfaceAudience.Public
  public abstract boolean getIsCreate();
}
