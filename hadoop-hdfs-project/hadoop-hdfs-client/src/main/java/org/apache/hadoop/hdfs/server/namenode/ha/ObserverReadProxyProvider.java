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

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.io.retry.RetryPolicy.RetryAction;

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

  /** The policy used to determine if an exception is fatal or retriable. */
  private final RetryPolicy observerRetryPolicy;

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

    observerProxies = resolveNameNodes(uri,
            DFSUtilClient.getObserverRpcAddresses(conf).get(uri.getHost()),
            HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_NEEDED_OBSERVER_KEY,
            HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_RANDOM_ORDER);
    // Max retry is not actually used when deciding retry
    // action as the number of retries are not counted.
    // The sleep base and max are used to make the client retry slower.
    DfsClientConf config = new DfsClientConf(conf);
    observerRetryPolicy = RetryPolicies.failoverOnNetworkException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, config.getMaxFailoverAttempts(),
        config.getMaxRetryAttempts(), config.getFailoverSleepBaseMillis(),
        config.getFailoverSleepMaxMillis());

    observerReadEnabled = conf.getBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED,
        HdfsClientConfigKeys.DFS_CLIENT_OBSERVER_READS_ENABLED_DEFAULT);

    if (observerReadEnabled) {
      LOG.debug("Reading from observer namenode is enabled");
    }
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

  // Check if the exception 'ex' wraps a FileNotFoundException or AccessControlException
  // with which, retrying the same action on active NameNode.
  private boolean isRetryActiveException(Exception ex, Class<? extends Exception>... exceptionTypes) {
    Throwable e = ex.getCause();
    if (e instanceof RemoteException) {
      RemoteException re = (RemoteException) e;

      for (Class<? extends Exception> exceptionType : exceptionTypes) {
        if (re.unwrapRemoteException().getClass() == exceptionType) {
          return true;
        }
      }
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

  /**
   * Whether we should direct the method being invoked to observer.
   */
  private boolean useObserver(final Method method) {
    return this.observerReadEnabled && Client.getObserverRead()
        && isRead(method);
  }

  @VisibleForTesting
  public ProxyInfo getLastProxy() {
    return lastProxy;
  }

  @VisibleForTesting
  public RetryPolicy getObserverRetryPolicy() {
    return observerRetryPolicy;
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

    /**
     * Sends read operations to the first observer NN (if enabled), and
     * send write operations to the active NN. If a observer NN fails, it is sent
     * to the back of the queue and the next is retried. If all observers fail,
     * we re-probe all the NNs and retry on the active.
     */
    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
        throws Throwable {
      lastProxy = null;
      boolean retryActiveOnException = false; // retry active namenode on certain exceptions
      Object retVal;

      if (useObserver(method)) {
        int start = usableProxyIndex.get();
        int failedObserverCount = 0;

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
              retryActiveOnException = true;
              break;
            }
            return retVal;
          } catch (InvocationTargetException ite) {
            if (!(ite.getCause() instanceof Exception)) {
              throw ite.getCause();
            }
            // If received remote FNF exception from server side (e.g., open)
            // or permission denied issue,
            // also break from the loop and try active.
            if (isRetryActiveException(ite,
                    FileNotFoundException.class,
                    AccessControlException.class)) {
              retryActiveOnException = true;
              break;
            }

            Exception e = (Exception) ite.getCause();
            RetryAction retryInfo = observerRetryPolicy.shouldRetry(e, 0,
                failedObserverCount,
                method.isAnnotationPresent(Idempotent.class)
                    || method.isAnnotationPresent(AtMostOnce.class));
            if (retryInfo.action == RetryAction.RetryDecision.FAIL) {
              throw e;
            } else {
              failedObserverCount++;
              LOG.warn(
                  "Invocation returned exception on [{}]; {} failure(s) so far",
                  current.proxyInfo, failedObserverCount, e);
              // add delay to make clients failover slower between observers
              Thread.sleep(retryInfo.delayMillis);
            }
          }
        }
      }

      // If we get here, it means all observer NNs have failed, or that it is a
      // write request. At this point we'll try to fail over to the active NN.
      try {
        if (!retryActiveOnException && useObserver(method)) {
          LOG.warn("All observers have failed for read request {}. Fall back "
              + "to active at: {}", method.getName(), activeProxy);
        }
        retVal = method.invoke(activeProxy.proxy, args);
        lastProxy = activeProxy;
        return retVal;
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
