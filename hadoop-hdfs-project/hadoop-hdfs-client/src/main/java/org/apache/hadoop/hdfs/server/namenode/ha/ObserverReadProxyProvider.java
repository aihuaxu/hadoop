/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.io.retry.MultiException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.hadoop.io.retry.FailoverProxyProvider} implementation
 * that supports reading from observer namenode(s).
 *
 * This constructs a wrapper proxy that sends the request to observer
 * namenode(s), if observer read is enabled (by setting
 * {@link HdfsClientConfigKeys#DFS_CLIENT_OBSERVER_READS_ENABLED} to true). In
 * case there are multiple observer namenodes, it will try them one by one in
 * case the RPC failed. It will fail back to the active namenode after it has
 * exhausted all the observer namenodes.
 *
 * Read and write requests will still be sent to active NN if
 * {@link HdfsClientConfigKeys#DFS_CLIENT_OBSERVER_READS_ENABLED} is set to
 * false.
 */
public class ObserverReadProxyProvider<T> extends ConfiguredFailoverProxyProvider<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ObserverReadProxyProvider.class);
  private static final String GET_LISTING = "getListing";
  private static final String GET_FILE_INFO = "getFileInfo";

  /** Proxies for the observer namenodes */
  private final List<AddressRpcProxyPair<T>> observerProxies;

  /**
   * Whether reading from observer has been enabled. If this is false, all read
   * requests will still go to active NN.
   */
  private final boolean observerReadEnabled;

  /** The last proxy that has been used. Only used for testing */
  private volatile ProxyInfo<T> lastProxy = null;

  public ObserverReadProxyProvider(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory) {
    super(conf, uri, xface, factory);

    // Initialize observer namenode list
    Map<String, Map<String, InetSocketAddress>> addressMap =
        DFSUtilClient.getObserverRpcAddresses(conf);
    Map<String, InetSocketAddress> addressesInNN = addressMap.get(uri.getHost());

    if (addressesInNN == null || addressesInNN.isEmpty()) {
      throw new RuntimeException("Could not find any configured observer " +
          "namenode address for URI " + uri);
    }

    observerProxies = new ArrayList<>();
    Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
    for (InetSocketAddress address : addressesOfNns) {
      observerProxies.add(new AddressRpcProxyPair<T>(address));
    }

    observerReadEnabled = conf.getBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED,
        HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED_DEFAULT);

    if (observerReadEnabled) {
      LOG.debug("Reading from observer namenode is enabled");
    }

    // The client may have a delegation token set for the logical
    // URI of the cluster. Clone this token to apply to each of the
    // underlying IPC addresses so that the IPC code can find it.
    // Copied from the parent class.
    HAUtilClient.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    // We just create a wrapped proxy containing all the proxies
    List<ProxyInfo<T>> observerProxies = new ArrayList<>();
    StringBuilder combinedInfo = new StringBuilder("[");

    for (int i = 0; i < this.observerProxies.size(); i++) {
      if (i > 0) {
        combinedInfo.append(",");
      }
      AddressRpcProxyPair<T> p = this.observerProxies.get(i);
      ProxyInfo<T> pInfo = getProxyInfo(p);
      observerProxies.add(pInfo);
      combinedInfo.append(pInfo.proxyInfo);
    }

    combinedInfo.append(']');
    T wrappedProxy = (T) Proxy.newProxyInstance(
        ObserverReadInvocationHandler.class.getClassLoader(),
        new Class<?>[]{xface},
        new ObserverReadInvocationHandler(observerProxies));
    return new ProxyInfo<>(wrappedProxy, combinedInfo.toString());
  }

  /**
   * Check the exception returned by the proxy log a warning message if it's
   * not a StandbyException (expected exception).
   * @param ex exception to evaluate.
   * @param proxyInfo information of the proxy reporting the exception.
   */
  private void logProxyException(Exception ex, String proxyInfo) {
    StandbyException se = unwrapStandbyException(ex);
    if (se != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Invocation returned standby exception on [" +
            proxyInfo + "]");
      }
    } else {
      LOG.warn("Invocation returned exception on [" + proxyInfo + "]", ex);
    }
  }

  /**
   * Check if the returned exception is caused by an standby namenode. If so,
   * return the wrapped StandbyException.
   * @param ex exception to check.
   * @return a non-null StandbyException wrapped in the input exception,
   *         or null if none found.
   */
  private StandbyException unwrapStandbyException(Exception ex) {
    Throwable cause = ex.getCause();
    while (cause != null) {
      if (cause instanceof RemoteException) {
        RemoteException remoteException = (RemoteException) cause;
        IOException unwrapRemoteException = remoteException.unwrapRemoteException();
        if (unwrapRemoteException instanceof StandbyException) {
          return (StandbyException) unwrapRemoteException;
        }
      }
      cause = cause.getCause();
    }
    return null;
  }

  /**
   * After getting exception 'ex', whether we should retry the current request
   * on a different observer.
   * TODO: make sure this fully covers all possible exceptions
   * TODO: perhaps we can leverage RetryPolicies.
   */
  private boolean shouldRetry(Exception ex) {
    Throwable e = ex.getCause();
    return (e instanceof ConnectException ||
        e instanceof EOFException ||
        e instanceof NoRouteToHostException ||
        e instanceof UnknownHostException ||
        e instanceof StandbyException ||
        e instanceof ConnectTimeoutException ||
        unwrapStandbyException(ex) != null);
  }

  // Check if the exception 'ex' wraps a FileNotFoundException.
  private boolean isFNFException(Exception ex) {
    Throwable e = ex.getCause();
    if (e instanceof RemoteException) {
      RemoteException re = (RemoteException) e;
      return re.unwrapRemoteException() instanceof FileNotFoundException;
    }
    return false;
  }

  // Check if the method is either 'getListing' or 'getFileInfo'.
  private boolean isGetListingOrFileInfo(Method m) {
    if (m != null) {
      String methodName = m.getName();
      return methodName != null && (methodName.equals(GET_LISTING) ||
          methodName.equals(GET_FILE_INFO));
    }
    return false;
  }

  /**
   * Check if a method is read-only.
   * @return whether the 'method' is a read-only operation.
   */
  private boolean isRead(Method method) {
    return method.isAnnotationPresent(ReadOnly.class);
  }

  @VisibleForTesting
  public ProxyInfo getLastProxy() {
    return lastProxy;
  }

  class ObserverReadInvocationHandler implements InvocationHandler {
    final List<ProxyInfo<T>> observerProxies;
    final ProxyInfo<T> activeProxy;

    // Index for the first working proxy in the list. We increment this
    // when the current proxy is unavailable.
    private final AtomicInteger usableProxyIndex;

    ObserverReadInvocationHandler(List<ProxyInfo<T>> observerProxies) {
      this.observerProxies = observerProxies;
      this.activeProxy = ObserverReadProxyProvider.super.getProxy();
      this.usableProxyIndex = new AtomicInteger(0);
    }

    void handleInvokeException(Map<String, Exception> badResults,
        Exception e, String proxyInfo) {
      logProxyException(e, proxyInfo);
      StandbyException se = unwrapStandbyException(e);
      badResults.put(proxyInfo, se != null ? se : e);
    }

    /**
     * Sends read operations to the first observer NN (if enabled), and
     * send write operations to the active NN. If a observer NN fails, it is sent
     * to the back of the queue and the next is retried. If all observers fail,
     * we re-probe all the NNs and retry on the active.
     */
    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      Map<String, Exception> badResults = new HashMap<>();
      lastProxy = null;
      boolean isFNFError = false;
      Object retVal;

      if (observerReadEnabled && isRead(method)) {
        int start = usableProxyIndex.get();

        // Loop through all the proxies, starting from the current index.
        for (int i = 0; i < observerProxies.size(); i++, start++) {
          ProxyInfo<T> current =
              observerProxies.get(start % observerProxies.size());
          try {
            retVal = method.invoke(current.proxy, args);
            lastProxy = current;
            if (i != 0) {
              usableProxyIndex.set(start % observerProxies.size());
            }

            // Check if return value is null (meaning FNF for getListing &
            // getFileInfo). If so, we break from the loop and try active.
            if (retVal == null && isGetListingOrFileInfo(method)) {
              isFNFError = true;
              break;
            }
            return retVal;
          } catch (Exception e) {
            // If received remote FNF exception from server side (e.g., open),
            // also break from the loop and try active.
            if (isFNFException(e)) {
              isFNFError = true;
              break;
            }
            if (!shouldRetry(e)) {
              throw e;
            }
            handleInvokeException(badResults, e, current.proxyInfo);
          }
        }
      }

      // If we get here, it means all observer NNs have failed, or that it is a
      // write request. At this point we'll try to fail over to the active NN.
      try {
        if (!isFNFError && observerReadEnabled && isRead(method)) {
          LOG.warn("All ONNs have failed for read request " + method.getName() + ". "
              + "Fall back on active NN: " + activeProxy);
        }
        retVal = method.invoke(activeProxy.proxy, args);
        lastProxy = activeProxy;
        return retVal;
      } catch (Exception e) {
        handleInvokeException(badResults, e, activeProxy.proxyInfo);
      }

      // At this point we should have ALL bad results (Exceptions)
      // Or should have returned with successful result.
      if (badResults.size() == 1) {
        throw badResults.values().iterator().next();
      } else {
        throw new MultiException(badResults);
      }
    }
  }
}
