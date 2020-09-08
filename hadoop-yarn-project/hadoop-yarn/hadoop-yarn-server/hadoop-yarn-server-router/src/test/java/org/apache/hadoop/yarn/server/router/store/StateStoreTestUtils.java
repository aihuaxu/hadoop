package org.apache.hadoop.yarn.server.router.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

public final class StateStoreTestUtils {
  /** The State Store Driver implementation class for testing .*/

  private StateStoreTestUtils() {
    // Utility Class
  }

  /**
   * Create a new State Store configuration for a particular driver.
   *
   * @param clazz Class of the driver to create.
   * @return State Store configuration.
   */
  public static Configuration getStateStoreConfiguration(
      Class<? extends StateStoreDriver> clazz) {
    Configuration conf = new YarnConfiguration();
    return conf;
  }

  /**
   * Create a new State Store based on a configuration.
   *
   * @param configuration Configuration for the State Store.
   * @return New State Store service.
   * @throws IOException If it cannot create the State Store.
   * @throws InterruptedException If we cannot wait for the store to start.
   */
  public static RouterStateStoreService newStateStore(
      Configuration configuration) throws IOException, InterruptedException {

    RouterStateStoreService stateStore = new RouterStateStoreService();
    assertNotNull(stateStore);

    // Set unique identifier, this is normally the router address
    String identifier = UUID.randomUUID().toString();
    stateStore.setIdentifier(identifier);

    stateStore.init(configuration);
    stateStore.start();
    assertTrue(stateStore.isDriverReady());
    // Wait for state store to connect
//    waitStateStore(stateStore, TimeUnit.SECONDS.toMillis(10));

    return stateStore;
  }

  /**
   * Wait for the State Store to initialize its driver.
   *
   * @param stateStore State Store.
   * @param timeoutMs Time out in milliseconds.
   * @throws IOException If the State Store cannot be reached.
   * @throws InterruptedException If the sleep is interrupted.
   */
  public static void waitStateStore(RouterStateStoreService stateStore,
      long timeoutMs) throws IOException, InterruptedException {
    long startingTime = Time.monotonicNow();
    while (!stateStore.isDriverReady()) {
      Thread.sleep(100);
      if (Time.monotonicNow() - startingTime > timeoutMs) {
        throw new IOException("Timeout waiting for State Store to connect");
      }
    }
  }

  /**
   * Clear records from a certain type from the State Store.
   *
   * @param store State Store to remove records from.
   * @param recordClass Class of the records to remove.
   * @return If the State Store was cleared.
   * @throws IOException If it cannot clear the State Store.
   */
  public static <T extends BaseRecord> boolean clearRecords(
      RouterStateStoreService store, Class<T> recordClass) throws IOException {
    List<T> emptyList = new ArrayList<>();
    if (!synchronizeRecords(store, emptyList, recordClass)) {
      return false;
    }
    store.refreshCaches(true);
    return true;
  }

  /**
   * Synchronize a set of records. Remove all and keep the ones specified.
   *
   * @param stateStore State Store service managing the driver.
   * @param records Records to add.
   * @param clazz Class of the record to synchronize.
   * @return If the synchronization succeeded.
   * @throws IOException If it cannot connect to the State Store.
   */
  public static <T extends BaseRecord> boolean synchronizeRecords(
      RouterStateStoreService stateStore, List<T> records, Class<T> clazz)
      throws IOException {
    StateStoreDriver driver = stateStore.getDriver();
    driver.verifyDriverReady();
    if (driver.removeAll(clazz)) {
      if (driver.putAll(records, true, false)) {
        return true;
      }
    }
    return false;
  }
}
