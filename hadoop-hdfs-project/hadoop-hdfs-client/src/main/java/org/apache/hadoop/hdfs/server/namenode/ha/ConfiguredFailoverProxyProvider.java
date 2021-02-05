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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FailoverProxyProvider implementation which allows one to configure
 * multiple URIs to connect to during fail-over. A random configured address is
 * tried first, and on a fail-over event the other addresses are tried
 * sequentially in a random order.
 */
public class ConfiguredFailoverProxyProvider<T> extends
    AbstractNNFailoverProxyProvider<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConfiguredFailoverProxyProvider.class);

  protected final Configuration conf;
  protected final List<AddressRpcProxyPair<T>> proxies;
  protected final UserGroupInformation ugi;
  protected final Class<T> xface;

  private int currentProxyIndex = 0;
  protected final HAProxyFactory<T> factory;

  public ConfiguredFailoverProxyProvider(
          Configuration conf,
          URI uri,
          Class<T> xface,
          HAProxyFactory<T> factory) {
    this.factory = factory;
    this.xface = xface;
    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);

    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        HdfsClientConfigKeys.Failover.CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
            CommonConfigurationKeysPublic
                    .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            maxRetriesOnSocketTimeouts);
    try {
      ugi = UserGroupInformation.getCurrentUser();
      this.proxies = resolveNameNodes(uri,
              DFSUtilClient.getHaNnRpcAddresses(conf).get(uri.getHost()),
              HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_NEEDED_KEY,
              HdfsClientConfigKeys.Failover.RANDOM_ORDER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Given name service uri and namenode addresses (single DNS or real addresses),
   * resolve to real NameNode addresses.
   * Randomize the list if requested.
   * @param uri name service
   */
  protected List<AddressRpcProxyPair<T>> resolveNameNodes(
          URI uri,
          Map<String, InetSocketAddress> namenodeAddresses,
          String resolveAddressNeededKey,
          String randomOrderKey) {
    final List<AddressRpcProxyPair<T>> proxies = new ArrayList<>();

    if (namenodeAddresses == null || namenodeAddresses.size() == 0) {
      throw new RuntimeException("Could not find any configured addresses " +
              "for URI " + uri);
    }

    Collection<InetSocketAddress> addressesOfNns = namenodeAddresses.values();
    try {
      addressesOfNns = getResolvedHostsIfNecessary(addressesOfNns, uri, resolveAddressNeededKey);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (InetSocketAddress address : addressesOfNns) {
      proxies.add(new AddressRpcProxyPair<T>(address));
    }
    // Randomize the list to prevent all clients pointing to the same one
    boolean randomized = getRandomOrder(conf,
            uri,
            randomOrderKey);
    if (randomized) {
      Collections.shuffle(proxies);
    }

    // The client may have a delegation token set for the logical
    // URI of the cluster. Clone this token to apply to each of the
    // underlying IPC addresses so that the IPC code can find it.
    HAUtilClient.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    return proxies;
  }

  /**
   * If resolved is needed: for every domain name in the parameter list,
   * resolve them into the actual IP addresses.
   *
   * @param addressesOfNns The domain name list from config.
   * @param nameNodeUri The URI of namenode/nameservice.
   * @
   * @return The collection of resolved IP addresses.
   * @throws IOException If there are issues resolving the addresses.
   */
  Collection<InetSocketAddress> getResolvedHostsIfNecessary(
          Collection<InetSocketAddress> addressesOfNns,
          URI nameNodeUri,
          String resolveAddressNeededKey)
      throws IOException {
    // 'host' here is usually the ID of the nameservice when address
    // resolving is needed.
    String host = nameNodeUri.getHost();
    String configKeyWithHost = resolveAddressNeededKey + "." + host;
    boolean resolveNeeded = conf.getBoolean(configKeyWithHost,
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_NEEDED_DEFAULT);
    if (!resolveNeeded) {
      // Early return is no resolve is necessary
      return addressesOfNns;
    }
    // decide whether to access server by IP or by host name
    String useFQDNKeyWithHost =
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_TO_FQDN + "." + host;
    boolean requireFQDN = conf.getBoolean(useFQDNKeyWithHost,
        HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_TO_FQDN_DEFAULT);

    Collection<InetSocketAddress> addressOfResolvedNns = new ArrayList<>();
    DomainNameResolver dnr = DomainNameResolverFactory.newInstance(
        conf, nameNodeUri, HdfsClientConfigKeys.Failover.RESOLVE_SERVICE_KEY);
    // If the address needs to be resolved, get all of the IP addresses
    // from this address and pass them into the proxy
    LOG.info("Namenode domain name will be resolved with {}",
        dnr.getClass().getName());
    for (InetSocketAddress address : addressesOfNns) {
      String[] resolvedHostNames = dnr.getAllResolvedHostnameByDomainName(
          address.getHostName(), requireFQDN);
      int port = address.getPort();
      for (String hostname : resolvedHostNames) {
        InetSocketAddress resolvedAddress = new InetSocketAddress(
            hostname, port);
        addressOfResolvedNns.add(resolvedAddress);
      }
    }

    return addressOfResolvedNns;
  }

  /**
   * Check whether random order is configured for failover proxy provider
   * for the namenode/nameservice.
   *
   * @param conf Configuration
   * @param nameNodeUri The URI of namenode/nameservice
   * @param randomOrderKey  the key to Configuration for random order
   * @return random order configuration
   */
  private static boolean getRandomOrder(
      Configuration conf, URI nameNodeUri, String randomOrderKey) {
    String host = nameNodeUri.getHost();
    String configKeyWithHost = randomOrderKey + "." + host;

    if (conf.get(configKeyWithHost) != null) {
      return conf.getBoolean(
          configKeyWithHost,
          HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
    }

    return conf.getBoolean(
        randomOrderKey,
        HdfsClientConfigKeys.Failover.RANDOM_ORDER_DEFAULT);
  }

  @Override
  public Class<T> getInterface() {
    return xface;
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    AddressRpcProxyPair<T> current = proxies.get(currentProxyIndex);
    return getProxyInfo(current);
  }

  protected ProxyInfo<T> getProxyInfo(AddressRpcProxyPair<T> p) {
    if (p.namenode == null) {
      try {
        p.namenode = factory.createProxy(conf,
            p.address, xface, ugi, false, getFallbackToSimpleAuth());
      } catch (IOException e) {
        LOG.error("Failed to create RPC proxy to NameNode", e);
        throw new RuntimeException(e);
      }
    }
    return new ProxyInfo<>(p.namenode, p.address.toString());
  }

  @Override
  public  void performFailover(T currentProxy) {
    incrementProxyIndex();
  }

  synchronized void incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }

  /**
   * A little pair object to store the address and connected RPC proxy object to
   * an NN. Note that {@link AddressRpcProxyPair#namenode} may be null.
   */
  protected static class AddressRpcProxyPair<T> {
    public final InetSocketAddress address;
    public T namenode;

    public AddressRpcProxyPair(InetSocketAddress address) {
      this.address = address;
    }
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (AddressRpcProxyPair<T> proxy : proxies) {
      if (proxy.namenode != null) {
        if (proxy.namenode instanceof Closeable) {
          ((Closeable)proxy.namenode).close();
        } else {
          RPC.stopProxy(proxy.namenode);
        }
      }
    }
  }

  /**
   * Logical URI is required for this failover proxy provider.
   */
  @Override
  public boolean useLogicalURI() {
    return true;
  }
}
