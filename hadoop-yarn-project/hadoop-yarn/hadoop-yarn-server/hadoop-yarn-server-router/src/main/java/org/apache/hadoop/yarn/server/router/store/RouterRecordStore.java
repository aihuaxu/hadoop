package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.store.CachedRecordStore;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;

import java.io.IOException;

/**
 * Management API for
 * {@link RouterState
 *  RouterState} records in the state store. Accesses the data store via the
 * {@link StateStoreDriver StateStoreDriver} interface. No data is cached.
 *
 * Similar implementation as HDFS router 3.1.0
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RouterRecordStore extends CachedRecordStore<RouterState> {

  public RouterRecordStore(StateStoreDriver driver) {
    super(RouterState.class, driver, true);
  }
}
