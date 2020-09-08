package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;

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
}
