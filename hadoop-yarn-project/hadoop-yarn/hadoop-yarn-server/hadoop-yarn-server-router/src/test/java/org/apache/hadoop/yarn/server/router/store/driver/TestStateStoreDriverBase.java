package org.apache.hadoop.yarn.server.router.store.driver;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.QueryResult;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.router.RouterServiceState;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.server.router.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils.getStateStoreConfiguration;
import static org.apache.hadoop.yarn.server.router.store.StateStoreTestUtils.newStateStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStateStoreDriverBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStateStoreDriverBase.class);

  private static RouterStateStoreService stateStore;
  private static Configuration conf;

  private static final Random RANDOM = new Random();


  /**
   * Get the State Store driver.
   * @return State Store driver.
   */
  protected StateStoreDriver getStateStoreDriver() {
    return stateStore.getDriver();
  }

  public static RouterStateStoreService fetchStateStore() {
    return stateStore;
  }

  public static void initDriverConnection(TestingServer curatorTestingServer,
      CuratorFramework curatorFramework) throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    String connectString = curatorTestingServer.getConnectString();
    curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();

    // Create the ZK State Store
    Configuration conf =
        getStateStoreConfiguration(StateStoreZooKeeperImpl.class);
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);
    getStateStore(conf);
  }

  /**
   * Get a new State Store using this configuration.
   *
   * @param config Configuration for the State Store.
   * @throws Exception If we cannot get the State Store.
   */
  public static void getStateStore(Configuration config) throws Exception {
    conf = config;
    stateStore = newStateStore(conf);
  }

  public static void removeAll(StateStoreDriver driver) throws IOException {
    driver.removeAll(RouterState.class);
  }

  public void testInsert(StateStoreDriver driver)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    testInsert(driver, RouterState.class);
  }

  public <T extends BaseRecord> void testInsert(
      StateStoreDriver driver, Class<T> recordClass)
      throws IllegalArgumentException, IllegalAccessException, IOException {

    assertTrue(driver.removeAll(recordClass));
    QueryResult<T> queryResult0 = driver.get(recordClass);
    List<T> records0 = queryResult0.getRecords();
    assertTrue(records0.isEmpty());

    // Insert single
    BaseRecord record = generateFakeRecord(recordClass);
    driver.put(record, true, false);

    // Verify
    QueryResult<T> queryResult1 = driver.get(recordClass);
    List<T> records1 = queryResult1.getRecords();
    assertEquals(1, records1.size());
    T record0 = records1.get(0);
    validateRecord(record, record0, true);

    // Insert multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(recordClass);
      insertList.add(newRecord);
    }
    driver.putAll(insertList, true, false);

    // Verify
    QueryResult<T> queryResult2 = driver.get(recordClass);
    List<T> records2 = queryResult2.getRecords();
    assertEquals(11, records2.size());
  }

  /**
   * Validate if a record is the same.
   *
   * @param original Original record.
   * @param committed Committed record.
   * @param assertEquals Assert if the records are equal or just return.
   * @return If the record is successfully validated.
   */
  private boolean validateRecord(
      BaseRecord original, BaseRecord committed, boolean assertEquals) {

    boolean ret = true;

    Map<String, Class<?>> fields = getFields(original);
    for (String key : fields.keySet()) {
      if (key.equals("dateModified") ||
          key.equals("dateCreated") ||
          key.equals("proto")) {
        // Fields are updated/set on commit and fetch and may not match
        // the fields that are initialized in a non-committed object.
        continue;
      }
      Object data1 = getField(original, key);
      Object data2 = getField(committed, key);
      if (assertEquals) {
        assertEquals("Field " + key + " does not match", data1, data2);
      } else if (!data1.equals(data2)) {
        ret = false;
      }
    }

    long now = stateStore.getDriver().getTime();
    assertTrue(
        committed.getDateCreated() <= now && committed.getDateCreated() > 0);
    assertTrue(committed.getDateModified() >= committed.getDateCreated());

    return ret;
  }

  /**
   * Returns all serializable fields in the object.
   *
   * @return Map with the fields.
   */
  private static Map<String, Class<?>> getFields(BaseRecord record) {
    Map<String, Class<?>> getters = new HashMap<>();
    for (Method m : record.getClass().getDeclaredMethods()) {
      if (m.getName().startsWith("get")) {
        try {
          Class<?> type = m.getReturnType();
          char[] c = m.getName().substring(3).toCharArray();
          c[0] = Character.toLowerCase(c[0]);
          String key = new String(c);
          getters.put(key, type);
        } catch (Exception e) {
          LOG.error("Cannot execute getter " + m.getName()
              + " on object " + record);
        }
      }
    }
    return getters;
  }

  /**
   * Finds the appropriate getter for a field name.
   *
   * @param fieldName The legacy name of the field.
   * @return The matching getter or null if not found.
   */
  private static Method locateGetter(BaseRecord record, String fieldName) {
    for (Method m : record.getClass().getMethods()) {
      if (m.getName().equalsIgnoreCase("get" + fieldName)) {
        return m;
      }
    }
    return null;
  }

  /**
   * Fetches the value for a field name.
   *
   * @param fieldName the legacy name of the field.
   * @return The field data or null if not found.
   */
  private static Object getField(BaseRecord record, String fieldName) {
    Object result = null;
    Method m = locateGetter(record, fieldName);
    if (m != null) {
      try {
        result = m.invoke(record);
      } catch (Exception e) {
        LOG.error("Cannot get field " + fieldName + " on object " + record);
      }
    }
    return result;
  }


  @SuppressWarnings("unchecked")
  private <T extends BaseRecord> T generateFakeRecord(Class<T> recordClass)
      throws IllegalArgumentException, IllegalAccessException, IOException {

    if (recordClass == RouterState.class) {
      RouterState routerState = RouterState.newInstance(generateRandomString(),
          generateRandomLong(), generateRandomEnum(RouterServiceState.class));
      return (T) routerState;
    }

    return null;
  }

  private String generateRandomString() {
    return "randomString-" + RANDOM.nextInt();
  }

  private long generateRandomLong() {
    return RANDOM.nextLong();
  }
  @SuppressWarnings("rawtypes")
  private <T extends Enum> T generateRandomEnum(Class<T> enumClass) {
    int x = RANDOM.nextInt(enumClass.getEnumConstants().length);
    T data = enumClass.getEnumConstants()[x];
    return data;
  }
}
