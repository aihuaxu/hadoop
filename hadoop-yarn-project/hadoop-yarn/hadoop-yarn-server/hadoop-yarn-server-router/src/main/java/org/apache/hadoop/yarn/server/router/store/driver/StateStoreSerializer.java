package org.apache.hadoop.yarn.server.router.store.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;

import java.io.IOException;

/**
 * Serializer to store and retrieve data in the State Store.
 * Similar implementation as HDFS router 3.1.0
 */
public abstract class StateStoreSerializer {

  /** Singleton for the serializer instance. */
  private static StateStoreSerializer defaultSerializer;

  /**
   * Get the default serializer based.
   * @return Singleton serializer.
   */
  public static StateStoreSerializer getSerializer() {
    return getSerializer(null);
  }

  /**
   * Get a serializer based on the provided configuration.
   * @param conf Configuration. Default if null.
   * @return Singleton serializer.
   */
  public static StateStoreSerializer getSerializer(Configuration conf) {
    if (conf == null) {
      synchronized (StateStoreSerializer.class) {
        if (defaultSerializer == null) {
          conf = new Configuration();
          defaultSerializer = newSerializer(conf);
        }
      }
      return defaultSerializer;
    } else {
      return newSerializer(conf);
    }
  }

  private static StateStoreSerializer newSerializer(final Configuration conf) {
    Class<? extends StateStoreSerializer> serializerName = conf.getClass(
        RouterConfigKeys.ROUTER_STORE_SERIALIZER_CLASS,
        RouterConfigKeys.ROUTER_STORE_SERIALIZER_CLASS_DEFAULT,
        StateStoreSerializer.class);
    return ReflectionUtils.newInstance(serializerName, conf);
  }

  /**
   * Create a new record.
   * @param clazz Class of the new record.
   * @return New record.
   */
  public static <T> T newRecord(Class<T> clazz) {
    return getSerializer(null).newRecordInstance(clazz);
  }

  /**
   * Create a new record.
   * @param clazz Class of the new record.
   * @return New record.
   */
  public abstract <T> T newRecordInstance(Class<T> clazz);

  /**
   * Serialize a record into a byte array.
   * @param record Record to serialize.
   * @return Byte array with the serialized record.
   */
  public abstract byte[] serialize(BaseRecord record);

  /**
   * Serialize a record into a string.
   * @param record Record to serialize.
   * @return String with the serialized record.
   */
  public abstract String serializeString(BaseRecord record);

  /**
   * Deserialize a bytes array into a record.
   * @param byteArray Byte array to deserialize.
   * @param clazz Class of the record.
   * @return New record.
   * @throws IOException If it cannot deserialize the record.
   */
  public abstract <T extends BaseRecord> T deserialize(
      byte[] byteArray, Class<T> clazz) throws IOException;

  /**
   * Deserialize a string into a record.
   * @param data String with the data to deserialize.
   * @param clazz Class of the record.
   * @return New record.
   * @throws IOException If it cannot deserialize the record.
   */
  public abstract <T extends BaseRecord> T deserialize(
      String data, Class<T> clazz) throws IOException;

}
