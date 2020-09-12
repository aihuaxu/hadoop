package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.store.exception.StateStoreUnavailableException;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.router.RouterServiceState;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils.clearRecords;
import static org.apache.hadoop.yarn.server.router.store.driver.TestStateStoreDriverBase.fetchStateStore;
import static org.junit.Assert.assertEquals;
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

  @Test
  public void testStateStoreDisconnected() throws Exception {

    // Close the data store driver
    getStateStoreService().closeDriver();
    assertEquals(false, getStateStoreService().isDriverReady());

    RouterHeartbeatRequest hbRequest = RouterHeartbeatRequest.newInstance(
        RouterState.newInstance("test", 0, RouterServiceState.UNINITIALIZED));
    StateStoreTestUtils.verifyException(routerRecordStore, "routerHeartbeat",
        StateStoreUnavailableException.class,
        new Class[] {RouterHeartbeatRequest.class},
        new Object[] {hbRequest});
  }

  @Test
  public void testUpdateRouterStatus()
      throws IllegalStateException, IOException {

    long dateStarted = Time.now();
    String address = "testaddress";

    // Set
    RouterHeartbeatRequest request = RouterHeartbeatRequest.newInstance(
        RouterState.newInstance(
            address, dateStarted, RouterServiceState.RUNNING));
    assertTrue(routerRecordStore.routerHeartbeat(request).getStatus());
  }

  @Test
  public void testRouterStateExpired()
      throws IOException, InterruptedException {

    long dateStarted = Time.now();
    String address = "testaddress";

    RouterHeartbeatRequest request = RouterHeartbeatRequest.newInstance(
        RouterState.newInstance(
            address, dateStarted, RouterServiceState.RUNNING));
    // Set
    assertTrue(routerRecordStore.routerHeartbeat(request).getStatus());

    // Wait past expiration (set to 5 sec in config)
    Thread.sleep(6000);

    // Heartbeat again and this shouldn't be EXPIRED anymore
    assertTrue(routerRecordStore.routerHeartbeat(request).getStatus());
  }

  @Test
  public void testGetAllRouterStates()
      throws StateStoreUnavailableException, IOException {

    // Set 2 entries
    RouterHeartbeatRequest heartbeatRequest1 =
        RouterHeartbeatRequest.newInstance(
            RouterState.newInstance(
                "testaddress1", Time.now(), RouterServiceState.RUNNING));
    assertTrue(routerRecordStore.routerHeartbeat(heartbeatRequest1).getStatus());

    RouterHeartbeatRequest heartbeatRequest2 =
        RouterHeartbeatRequest.newInstance(
            RouterState.newInstance(
                "testaddress2", Time.now(), RouterServiceState.RUNNING));
    assertTrue(routerRecordStore.routerHeartbeat(heartbeatRequest2).getStatus());
  }

}
