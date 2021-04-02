/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Time;

/**
 * Context to track a connection in a {@link ConnectionPool}. When a client uses
 * a connection, it increments a counter to mark it as active. Once the client
 * is done with the connection, it decreases the counter. It also takes care of
 * closing the connection once is not active.
 */
public class ConnectionContext {

  /** Client for the connection. */
  private final ProxyAndInfo<ClientProtocol> client;
  /** How many threads are using this connection. */
  private final AtomicInteger numThreads;
  /** If the connection is closed. */
  private boolean closed = false;
  /** Last timestamp the connection was active. */
  private long lastActiveTs = 0;
  /** The connection's active status would expire after this window */
  private final static long ACTIVE_WINDOW_TIME = TimeUnit.SECONDS.toMillis(30);


  public ConnectionContext(ProxyAndInfo<ClientProtocol> connection) {
    this.client = connection;
    numThreads = new AtomicInteger(0);
  }

  /**
   * Check if the connection is active.
   *
   * @return True if the connection is active.
   */
  public synchronized boolean isActive() {
    return this.numThreads.get() > 0;
  }

  /**
   * Check if the connection is/was active recently.
   *
   * @return True if the connection is active or
   * was active in the past period of time.
   */
  public synchronized boolean isActiveRecently() {
    return Time.monotonicNow() - this.lastActiveTs <= ACTIVE_WINDOW_TIME;
  }

  /**
   * Check if the connection is closed.
   *
   * @return If the connection is closed.
   */
  public synchronized boolean isClosed() {
    return this.closed;
  }

  /**
   * Check if the connection can be used. It checks if the connection is used by
   * another thread or already closed.
   *
   * @return True if the connection can be used.
   */
  public synchronized boolean isUsable() {
    return !isActive() && !isClosed();
  }

  /**
   * Get the connection client.
   *
   * @return Connection client.
   */
  public synchronized ProxyAndInfo<ClientProtocol> getClient() {
    this.numThreads.incrementAndGet();
    this.lastActiveTs = Time.monotonicNow();
    System.out.println("XXX: number threads" + numThreads);
    return this.client;
  }

  /**
   * Release this connection.
   */
  public synchronized void release() {
    this.numThreads.updateAndGet(x -> (x > 0 ? x-1 : x) );
  }

  /**
   * Close a connection. Only idle connections can be closed since
   * the RPC proxy would be shut down immediately.
   *
   * @param force whether the connection should be closed anyway.
   * @throws IllegalStateException when the connection is not idle
   */

  public synchronized void close(boolean force) throws IllegalStateException {
    if (!force && this.numThreads.get() > 0) {
      throw new IllegalStateException("Active connection cannot be closed");
    }
    this.closed = true;
    Object proxy = this.client.getProxy();
    // Nobody should be using this anymore so it should close right away
    RPC.stopProxy(proxy);
  }

  public synchronized void close() {
    close(false);
  }
}
