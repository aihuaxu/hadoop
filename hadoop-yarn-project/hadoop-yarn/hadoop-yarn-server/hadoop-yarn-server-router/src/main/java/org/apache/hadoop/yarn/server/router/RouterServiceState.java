package org.apache.hadoop.yarn.server.router;

/**
 * States of the Router.
 */
public enum RouterServiceState {
  UNINITIALIZED,
  INITIALIZING,
  SAFEMODE,
  RUNNING,
  STOPPING,
  SHUTDOWN,
  EXPIRED;
}
