package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
public class RouterRedirectProxyProvider extends ConfiguredFailoverProxyProvider<ClientProtocol> {
  private static final Logger LOG = LoggerFactory.getLogger(RouterRedirectProxyProvider.class);
  private static final String GET_FILE_INFO = "getFileInfo";

  private final RetryPolicy retryPolicy;

  public RouterRedirectProxyProvider(
          Configuration conf,
          URI uri,
          Class xface,
          HAProxyFactory<ClientProtocol> factory) {
    super(conf, uri, xface, factory);

    DfsClientConf config = new DfsClientConf(conf);
    retryPolicy = RetryPolicies.failoverOnNetworkException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, config.getMaxFailoverAttempts(),
            config.getMaxRetryAttempts(), config.getFailoverSleepBaseMillis(),
            config.getFailoverSleepMaxMillis());
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized ProxyInfo<ClientProtocol> getProxy() {
    // We just create a wrapped proxy containing all the proxies
    ClientProtocol wrappedProxy = (ClientProtocol) Proxy.newProxyInstance(
            RouterRedirectProxyProvider.RouterRedirectInvocationHandler.class.getClassLoader(),
            new Class<?>[]{xface},
            new RouterRedirectProxyProvider.RouterRedirectInvocationHandler());
    return new ProxyInfo<>(wrappedProxy, "RouterRedirect");
  }

  // TODO implements retry logic
  class RouterRedirectInvocationHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, final Method method, final Object[] args)
            throws Throwable {
      try {
        if (isFileInfo(method)) {
          return getFileInfo((String) args[0]);
        }

        return method.invoke(RouterRedirectProxyProvider.super.getProxy().proxy, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }

    /**
     * Fetches file status for the given path by resolving to physical path and
     * then getting file status from that physical cluster directly.
     * @param path  File path on the router
     * @return      File status of the path
     */
    private HdfsFileStatus getFileInfo(String path) throws Exception {
      Path resolvedPath = resolvePath(path);

      URI uri = resolvedPath.toUri();
      List<AddressRpcProxyPair<ClientProtocol>> physicalProxies =
              resolveNameNodes(uri,
                      DFSUtilClient.getHaNnRpcAddresses(conf).get(uri.getHost()),
                      HdfsClientConfigKeys.Failover.RESOLVE_ADDRESS_NEEDED_KEY,
                      HdfsClientConfigKeys.Failover.RANDOM_ORDER);

      int failovers = 0;
      int nnIndex = 0;
      while(true) {
        try {
          AddressRpcProxyPair<ClientProtocol> physicalProxy = physicalProxies.get(nnIndex);
          HdfsFileStatus fs = getProxyInfo(physicalProxy)
                  .proxy.getFileInfo(resolvedPath.toUri().getPath());

          if (fs == null) {
            return null; // new file
          }

          HdfsFileStatus newFS = new HdfsFileStatus(
                  fs.getLen(), fs.isDir(), fs.getReplication(),
                  fs.getBlockSize(), fs.getModificationTime(), fs.getAccessTime(),
                  fs.getPermission(), fs.getOwner(), fs.getGroup(),
                  fs.getSymlinkInBytes(), path.getBytes(), fs.getFileId(),
                  fs.getChildrenNum(), fs.getFileEncryptionInfo(),
                  fs.getStoragePolicy());

          return newFS;
        } catch (IOException ioe) {
          System.out.println("Exception in getFileInfo call:" + ioe.getMessage());
          RetryPolicy.RetryAction retryInfo = retryPolicy.shouldRetry(ioe, 0,
                  failovers, true);
          if (retryInfo.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
            throw ioe;
          } else {
            ++failovers;
            nnIndex = (++nnIndex) % physicalProxies.size();
          }
        }
      }
    }

    private Path resolvePath(String path) throws IOException {
      List<Path> locs =
              RouterRedirectProxyProvider.super.getProxy().proxy.getRemoteLocation(path);

      return locs.get(0);
    }

    private boolean isFileInfo(Method m) {
      if (m != null) {
        String methodName = m.getName();
        return methodName != null && methodName.equals(GET_FILE_INFO);
      }
      return false;
    }
  }
}
