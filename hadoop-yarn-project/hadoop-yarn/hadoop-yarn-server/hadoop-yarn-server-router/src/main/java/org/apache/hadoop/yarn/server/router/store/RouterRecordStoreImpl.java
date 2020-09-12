package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;

import java.io.IOException;

/**
 * Implementation of the {@link RouterRecordStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RouterRecordStoreImpl extends RouterRecordStore {

  public RouterRecordStoreImpl(StateStoreDriver driver) {
    super(driver);
  }

  @Override
  public String getRecordName(Class<? extends BaseRecord> cls) {
    return RouterServerUtil.getRecordName(cls);
  }

  @Override
  public RouterHeartbeatResponse routerHeartbeat(RouterHeartbeatRequest request)
      throws IOException {

    RouterState record = request.getRouter();
    boolean status = getDriver().put(record, true, false);
    RouterHeartbeatResponse response =
        RouterHeartbeatResponse.newInstance(status);
    return response;
  }
}
