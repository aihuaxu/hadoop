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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test {@link ObserverReadProxyProvider}.
 * This manages failover logic for a given set of nameservices/namenodes
 * (aka proxies).
 */
public class TestObserverReadProxyProvider {
  private Configuration conf;
  private int rpcPort = 8020;
  private URI ns1Uri;
  private URI ns2Uri;
  private String ns1;
  private String ns1nn1Hostname = "active.foo.bar";
  private InetSocketAddress ns1nn1 =
      new InetSocketAddress(ns1nn1Hostname, rpcPort);
  private String ns1ob1Hostname = "ob1.foo.bar";
  private InetSocketAddress ns1ob1 =
      new InetSocketAddress(ns1ob1Hostname, rpcPort);
  private String ns1ob2Hostname = "ob2.foo.bar";
  private InetSocketAddress ns1ob2 =
      new InetSocketAddress(ns1ob2Hostname, rpcPort);
  private String ns2;
  private String ns2r1Hostname = "rw-router1.foo.bar";
  private InetSocketAddress ns2r1 =
      new InetSocketAddress(ns2r1Hostname, rpcPort);
  private String ns2r2Hostname = "ro-router2.foo.bar";
  private InetSocketAddress ns2r2 =
      new InetSocketAddress(ns2r2Hostname, rpcPort);
  private String ns2r3Hostname = "ro-router3.foo.bar";
  private InetSocketAddress ns2r3 =
      new InetSocketAddress(ns2r3Hostname, rpcPort);

  private static final int NUM_ITERATIONS = 50;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setupClass() throws Exception {
    GenericTestUtils.setLogLevel(RequestHedgingProxyProvider.LOG, Level.TRACE);
  }

  @Before
  public void setup() throws URISyntaxException {
    ns1 = "mycluster-1-" + Time.monotonicNow();
    ns1Uri = new URI("hdfs://" + ns1);
    conf = new Configuration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED,
        true);

    conf.set(
        HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns1,
        "nn1,ob1,ob2");
    conf.set(
        HdfsClientConfigKeys.DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX + "." + ns1,
        "ob1,ob2");
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns1 + ".nn1",
        ns1nn1Hostname + ":" + rpcPort);
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns1 + ".ob1",
        ns1ob1Hostname + ":" + rpcPort);
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns1 + ".ob2",
        ns1ob2Hostname + ":" + rpcPort);
    conf.set(
        HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + ns1,
        ObserverReadProxyProvider.class.getName());
    conf.setBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_RANDOM_ORDER +
            "." + ns1, false);

    ns2 = "myroutercluster-2-" + Time.monotonicNow();
    ns2Uri = new URI("hdfs://" + ns2);
    conf.set(
        HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + ns2,
        "r1,r2,r3");
    conf.set(
        HdfsClientConfigKeys.DFS_HA_OBSERVER_NAMENODES_KEY_PREFIX + "." + ns2,
        "r2,r3");
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns2 + ".r1",
        ns2r1Hostname + ":" + rpcPort);
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns2 + ".r2",
        ns2r2Hostname + ":" + rpcPort);
    conf.set(
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + ns2 + ".r3",
        ns2r3Hostname + ":" + rpcPort);
    conf.set(
        HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + ns2,
        ObserverReadProxyProvider.class.getName());
    conf.setBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_RANDOM_ORDER +
            "." + ns2, true);

    conf.set(HdfsClientConfigKeys.DFS_NAMESERVICES,
        StringUtils.join(',', new String[]{ns1, ns2}));
    conf.set("fs.defaultFS", "hdfs://" + ns1);
  }

  /**
   * Tests getProxy with random.order configuration set to false.
   * This expects the proxy order to be consistent every time a new
   * ConfiguredFailoverProxyProvider is created.
   */
  @Test
  public void testNonRandomGetProxy() throws Exception {
    final AtomicInteger nn1Count = new AtomicInteger(0);
    final AtomicInteger ob1Count = new AtomicInteger(0);
    final AtomicInteger ob2Count = new AtomicInteger(0);

    Map<InetSocketAddress, ClientProtocol> proxyMap = new HashMap<>();

    final ClientProtocol nn1Mock = mock(ClientProtocol.class);
    when(nn1Mock.getStats()).thenAnswer(createAnswer(nn1Count, 0));
    proxyMap.put(ns1nn1, nn1Mock);

    final ClientProtocol ob1Mock = mock(ClientProtocol.class);
    when(ob1Mock.getStats()).thenAnswer(createAnswer(ob1Count, 1));
    proxyMap.put(ns1ob1, ob1Mock);

    final ClientProtocol ob2Mock = mock(ClientProtocol.class);
    when(ob2Mock.getStats()).thenAnswer(createAnswer(ob2Count, 2));
    proxyMap.put(ns1ob2, ob2Mock);

    ObserverReadProxyProvider<ClientProtocol> provider1 =
        new ObserverReadProxyProvider<>(conf, ns1Uri,
            ClientProtocol.class, createFactory(proxyMap));
    ClientProtocol proxy1 = provider1.getProxy().proxy;
    proxy1.getStats();
    assertEquals(0, nn1Count.get());
    assertEquals(1, ob1Count.get());
    assertEquals(0, ob2Count.get());
    proxy1.getStats();
    assertEquals(0, nn1Count.get());
    assertEquals(2, ob1Count.get());
    assertEquals(0, ob2Count.get());
    nn1Count.set(0);
    ob1Count.set(0);
    ob2Count.set(0);

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ObserverReadProxyProvider<ClientProtocol> provider2 =
          new ObserverReadProxyProvider<>(conf, ns1Uri,
              ClientProtocol.class, createFactory(proxyMap));
      ClientProtocol proxy2 = provider2.getProxy().proxy;
      proxy2.getStats();
    }
    assertEquals(0, nn1Count.get());
    assertEquals(NUM_ITERATIONS, ob1Count.get());
    assertEquals(0, ob2Count.get());
  }

  /**
   * Tests getProxy with random.order configuration set to true.
   * This expects the proxy order to be random every time a new
   * ConfiguredFailoverProxyProvider is created.
   */
  @Test
  public void testRandomGetProxy() throws Exception {
    final AtomicInteger nn1Count = new AtomicInteger(0);
    final AtomicInteger nn2Count = new AtomicInteger(0);
    final AtomicInteger nn3Count = new AtomicInteger(0);

    Map<InetSocketAddress, ClientProtocol> proxyMap = new HashMap<>();

    final ClientProtocol nn1Mock = mock(ClientProtocol.class);
    when(nn1Mock.getStats()).thenAnswer(createAnswer(nn1Count, 1));
    proxyMap.put(ns2r1, nn1Mock);

    final ClientProtocol nn2Mock = mock(ClientProtocol.class);
    when(nn2Mock.getStats()).thenAnswer(createAnswer(nn2Count, 2));
    proxyMap.put(ns2r2, nn2Mock);

    final ClientProtocol nn3Mock = mock(ClientProtocol.class);
    when(nn3Mock.getStats()).thenAnswer(createAnswer(nn3Count, 3));
    proxyMap.put(ns2r3, nn3Mock);


    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ObserverReadProxyProvider<ClientProtocol> provider =
          new ObserverReadProxyProvider<>(conf, ns2Uri,
              ClientProtocol.class, createFactory(proxyMap));
      ClientProtocol proxy = provider.getProxy().proxy;
      proxy.getStats();
    }

    assertTrue(nn1Count.get() == 0);
    assertTrue(nn2Count.get() < NUM_ITERATIONS && nn2Count.get() > 0);
    assertTrue(nn3Count.get() < NUM_ITERATIONS && nn3Count.get() > 0);
    assertEquals(NUM_ITERATIONS,
        nn1Count.get() + nn2Count.get() + nn3Count.get());
  }

  /**
   * Tests getProxy with proper delays between failovers.
   */
  @Test
  public void testDelayBetweenFailover() throws Exception {
    Map<InetSocketAddress, ClientProtocol> proxyMap = new HashMap<>();

    for (int i = 1; i <= NUM_ITERATIONS; i++) {
      ObserverReadProxyProvider<ClientProtocol> provider =
          new ObserverReadProxyProvider<>(conf, ns2Uri,
              ClientProtocol.class, createFactory(proxyMap));
      RetryPolicy.RetryAction action =
          provider.getObserverRetryPolicy()
              .shouldRetry(new StandbyException("test"), 0, i,true);
      if (i < 15) { // 15 is the failover max limit
        assertTrue(action.action
            == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY);
        assertTrue(action.delayMillis > 0);
      } else {
        assertTrue(action.action
            == RetryPolicy.RetryAction.RetryDecision.FAIL);
      }
    }
  }

  /**
   * createAnswer creates an Answer for using with the ClientProtocol mocks.
   * @param counter counter to increment
   * @param retVal return value from answer
   * @return
   */
  private Answer<long[]> createAnswer(final AtomicInteger counter,
                                      final long retVal) {
    return new Answer<long[]>() {
      @Override
      public long[] answer(InvocationOnMock invocation) throws Throwable {
        counter.incrementAndGet();
        return new long[]{retVal};
      }
    };
  }

  /**
   * createFactory returns a HAProxyFactory for tests.
   * This uses a map of name node address to ClientProtocol to route calls to
   * different ClientProtocol objects. The tests could create ClientProtocol
   * mocks and create name node mappings to use with
   * ConfiguredFailoverProxyProvider.
   */
  private HAProxyFactory<ClientProtocol> createFactory(
      final Map<InetSocketAddress, ClientProtocol> proxies) {
    final Map<InetSocketAddress, ClientProtocol> proxyMap = proxies;
    return new HAProxyFactory<ClientProtocol>() {
      @Override
      public ClientProtocol createProxy(Configuration cfg,
                                        InetSocketAddress nnAddr, Class<ClientProtocol> xface,
                                        UserGroupInformation ugi, boolean withRetries,
                                        AtomicBoolean fallbackToSimpleAuth) throws IOException {
        if (proxyMap.containsKey(nnAddr)) {
          return proxyMap.get(nnAddr);
        } else {
          throw new IOException("Name node address not found");
        }
      }

      @Override
      public ClientProtocol createProxy(Configuration cfg,
                                        InetSocketAddress nnAddr, Class<ClientProtocol> xface,
                                        UserGroupInformation ugi, boolean withRetries) throws IOException {
        if (proxyMap.containsKey(nnAddr)) {
          return proxyMap.get(nnAddr);
        } else {
          throw new IOException("Name node address not found");
        }
      }
    };
  }
}