package org.apache.hadoop.yarn.server.router.store.driver.impl;

import com.google.protobuf.Message;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Protobuf implementation of the State Store serializer.
 * Similar implementation as HDFS router 3.1.0
 */
public final class StateStoreSerializerPBImpl extends StateStoreSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(StateStoreSerializerPBImpl.class);
  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBImpl";

  private Configuration localConf = new Configuration();


  private StateStoreSerializerPBImpl() {
  }

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

  @Override
  @SuppressWarnings("unchecked")
  public <T> T newRecordInstance(Class<T> clazz) {
    try {
      String clazzPBImpl = getPBImplClassName(clazz);
      Class<?> pbClazz = localConf.getClassByName(clazzPBImpl);
      Object retObject = ReflectionUtils.newInstance(pbClazz, localConf);
      return (T)retObject;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }

  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }

  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }

  @Override
  public byte[] serialize(BaseRecord record) {
    byte[] byteArray64 = null;
    if (record instanceof PBRecord) {
      PBRecord recordPB = (PBRecord) record;
      Message msg = recordPB.getProto();
      byte[] byteArray = msg.toByteArray();
      byteArray64 = Base64.encodeBase64(byteArray, false);
    }
    return byteArray64;
  }

  @Override
  public String serializeString(BaseRecord record) {
    byte[] byteArray64 = serialize(record);
    String base64Encoded = StringUtils.newStringUtf8(byteArray64);
    return base64Encoded;
  }

  @Override
  public <T extends BaseRecord> T deserialize(
      byte[] byteArray, Class<T> clazz) throws IOException {

    T record = newRecord(clazz);
    if (record instanceof PBRecord) {
      PBRecord pbRecord = (PBRecord) record;
      byte[] byteArray64 = Base64.encodeBase64(byteArray, false);
      String base64Encoded = StringUtils.newStringUtf8(byteArray64);
      pbRecord.readInstance(base64Encoded);
    }
    return record;
  }

  @Override
  public <T extends BaseRecord> T deserialize(String data, Class<T> clazz)
      throws IOException {
    byte[] byteArray64 = Base64.decodeBase64(data);
    return deserialize(byteArray64, clazz);
  }

}
