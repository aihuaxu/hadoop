package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.yarn.server.router.store.records.RouterState;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils.clearRecords;
import static org.apache.hadoop.yarn.server.router.store.driver.TestStateStoreDriverBase.fetchStateStore;
import static org.junit.Assert.assertTrue;

public class TestRouterRecordStore extends TestRouterStateStoreService {
  private RouterRecordStore routerRecordStore;
  @Before
  public void setup() throws IOException, InterruptedException {
    if (routerRecordStore == null) {
      routerRecordStore =
          fetchStateStore().getRegisteredRecordStore(RouterRecordStore.class);
  }
    // Clear router status registrations
    assertTrue(clearRecords(fetchStateStore(), RouterState.class));
  }

  //
  // Router
  //
  @Test
  public void testUpdateRouterStatus()
      throws IllegalStateException, IOException {
//    TODO: will add after operations implementation in RouterRecordStore
  }
}
