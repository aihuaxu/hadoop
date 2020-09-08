package org.apache.hadoop.store.record;

import com.google.protobuf.Message;

import java.io.IOException;

/**
 * A record implementation using Protobuf.
 * Abstract from release-int-3.1.0 hdfs router
 */
public interface PBRecord {

  /**
   * Get the protocol for the record.
   * @return The protocol for this record.
   */
  Message getProto();

  /**
   * Set the protocol for the record.
   * @param proto Protocol for this record.
   */
  void setProto(Message proto);

  /**
   * Populate this record with serialized data.
   * @param base64String Serialized data in base64.
   * @throws IOException If it cannot read the data.
   */
  void readInstance(String base64String) throws IOException;
}
