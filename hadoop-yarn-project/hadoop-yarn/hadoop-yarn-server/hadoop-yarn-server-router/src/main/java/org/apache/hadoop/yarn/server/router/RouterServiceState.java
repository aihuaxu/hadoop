package org.apache.hadoop.yarn.server.router;

/**
 * States of the Router.
 */
public enum RouterServiceState {
  UNINITIALIZED,
  INITIALIZING,
  RUNNING,
  STOPPING,
  SHUTDOWN,
  EXPIRED;
}
