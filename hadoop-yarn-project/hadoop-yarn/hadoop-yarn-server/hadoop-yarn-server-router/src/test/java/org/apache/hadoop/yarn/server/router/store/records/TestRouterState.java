package org.apache.hadoop.yarn.server.router.store.records;

import org.apache.hadoop.yarn.server.router.RouterServiceState;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestRouterState {

  private static final String ADDRESS = "address";
  private static final String VERSION = "version";
  private static final String COMPILE_INFO = "compileInfo";
  private static final long START_TIME = 100;
  private static final long DATE_MODIFIED = 200;
  private static final long DATE_CREATED = 300;
  private static final long FILE_RESOLVER_VERSION = 500;
  private static final RouterServiceState STATE = RouterServiceState.RUNNING;


  private RouterState generateRecord() throws IOException {
    RouterState record = RouterState.newInstance(ADDRESS, START_TIME, STATE);
    record.setDateCreated(DATE_CREATED);
    record.setDateModified(DATE_MODIFIED);
    return record;
  }

  private void validateRecord(RouterState record) throws IOException {
    assertEquals(ADDRESS, record.getAddress());
    assertEquals(START_TIME, record.getDateStarted());
    assertEquals(STATE, record.getStatus());
  }

  @Test
  public void testGetterSetter() throws IOException {
    RouterState record = generateRecord();
    validateRecord(record);
  }

  @Test
  public void testSerialization() throws IOException {

    RouterState record = generateRecord();

    StateStoreSerializer serializer = StateStoreSerializer.getSerializer();
    String serializedString = serializer.serializeString(record);
    RouterState newRecord =
        serializer.deserialize(serializedString, RouterState.class);

    validateRecord(newRecord);
  }
}
