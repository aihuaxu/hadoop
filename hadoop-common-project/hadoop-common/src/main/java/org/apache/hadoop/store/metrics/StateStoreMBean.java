package org.apache.hadoop.store.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * JMX interface for the State Store metrics.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface StateStoreMBean {

  long getReadOps();

  double getReadAvg();

  long getWriteOps();

  double getWriteAvg();

  long getFailureOps();

  double getFailureAvg();

  long getRemoveOps();

  double getRemoveAvg();
}
