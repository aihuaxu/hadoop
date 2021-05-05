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

package org.apache.hadoop.yarn.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ConfiguredRMFailoverProxyProvider<T>
    implements RMFailoverProxyProvider<T> {
  private static final Log LOG =
      LogFactory.getLog(ConfiguredRMFailoverProxyProvider.class);

  private int currentProxyIndex = 0;
  Map<String, T> proxies = new HashMap<String, T>();

  private RMProxy<T> rmProxy;
  private Class<T> protocol;
  protected YarnConfiguration conf;
  protected List<String> rmServiceIds;

  @Override
  public void init(Configuration configuration, RMProxy<T> rmProxy,
                    Class<T> protocol) {
    this.rmProxy = rmProxy;
    this.protocol = protocol;
    this.rmProxy.checkAllowedProtocols(this.protocol);
    this.conf = new YarnConfiguration(configuration);
    Collection<String> rmIds = HAUtil.getRMHAIds(conf);
    if (rmIds == null || rmIds.isEmpty()) {
      String message = "no instances conofigured.";
      LOG.error(message);
      throw new IllegalStateException(message);
    }
    this.rmServiceIds = new ArrayList<>(rmIds);

    if (conf.getBoolean(YarnConfiguration.CLIENT_RANDOM_ORDER_ENABLED, true)) {
      // Randomize the list to prevent all clients pointing to the same one, enabled by default
      LOG.info("Random order enabled");
      Collections.shuffle(rmServiceIds);
    }
    conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds.get(currentProxyIndex));

    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES));

    conf.setInt(CommonConfigurationKeysPublic.
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS,
            YarnConfiguration.DEFAULT_CLIENT_FAILOVER_RETRIES_ON_SOCKET_TIMEOUTS));
  }

  private T getProxyInternal() {
    try {
      final InetSocketAddress rmAddress = rmProxy.getRMAddress(conf, protocol);
      return RMProxy.getProxy(conf, protocol, rmAddress);
    } catch (IOException ioe) {
      LOG.error("Unable to create proxy to the ResourceManager " +
          rmServiceIds.get(currentProxyIndex), ioe);
      return null;
    }
  }

  @Override
  public synchronized ProxyInfo<T> getProxy() {
    String rmId = rmServiceIds.get(currentProxyIndex);
    T current = proxies.get(rmId);
    if (current == null) {
      current = getProxyInternal();
      proxies.put(rmId, current);
    }
    LOG.info("Sending request to " + rmId);
    return new ProxyInfo<T>(current, rmId);
  }

  @Override
  public synchronized void performFailover(T currentProxy) {
    currentProxyIndex = (currentProxyIndex + 1) % rmServiceIds.size();
    conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds.get(currentProxyIndex));
    LOG.info("Failing over to " + rmServiceIds.get(currentProxyIndex));
  }

  @Override
  public Class<T> getInterface() {
    return protocol;
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (T proxy : proxies.values()) {
      if (proxy instanceof Closeable) {
        ((Closeable)proxy).close();
      } else {
        RPC.stopProxy(proxy);
      }
    }
  }
}
