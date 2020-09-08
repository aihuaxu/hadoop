package org.apache.hadoop.store.exception;

import java.io.IOException;

/**
 * Thrown when the state store is not reachable or available. Cached APIs and
 * queries may succeed. Client should retry again later.
 * Abstract from release-int-3.1.0 hdfs router
 */
public class StateStoreUnavailableException extends IOException {

  private static final long serialVersionUID = 1L;

  public StateStoreUnavailableException(String msg) {
    super(msg);
  }

}