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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class TestConfiguredRMFailoverProxyProvider {
  private Configuration conf;
  ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProvider;
  @Before
  public void testInit() {
    conf = new Configuration();
    conf.setBoolean(YarnConfiguration.CLIENT_RANDOM_ORDER_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2");
    RMProxy rm1Mock = mock(RMProxy.class);
    doNothing().when(rm1Mock).checkAllowedProtocols(ApplicationClientProtocol.class);
    proxyProvider = new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    proxyProvider.init(conf, rm1Mock, ApplicationClientProtocol.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testInitWithNoInstances() {
    Configuration conf = new Configuration();;
    RMProxy rm1Mock = mock(RMProxy.class);
    doNothing().when(rm1Mock).checkAllowedProtocols(ApplicationClientProtocol.class);
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProviderWithNoInstances = new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    proxyProviderWithNoInstances.init(conf, rm1Mock, ApplicationClientProtocol.class);
  }

  @Test
  public void testGetProxyWithRandomOrder() {
    proxyProvider.getProxy();
    Map<String, ApplicationClientProtocol> proxies =  proxyProvider.proxies;
    assertEquals(proxies.size(), 1);
  }

  @Test
  public void testGetProxyWithoutRandomOrder() {
    FailoverProxyProvider.ProxyInfo<ApplicationClientProtocol> proxy = proxyProvider.getProxy();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3,rm4");
    RMProxy rm1Mock = mock(RMProxy.class);
    doNothing().when(rm1Mock).checkAllowedProtocols(ApplicationClientProtocol.class);
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProviderWithNoInstances = new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    proxyProviderWithNoInstances.init(conf, rm1Mock, ApplicationClientProtocol.class);
    assertEquals(proxyProviderWithNoInstances.rmServiceIds.get(0), "rm1");
    FailoverProxyProvider.ProxyInfo<ApplicationClientProtocol> current = proxyProviderWithNoInstances.getProxy();
    assertEquals(current.proxyInfo, "rm1");
  }
}
