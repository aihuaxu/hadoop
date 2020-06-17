package org.apache.hadoop.yarn.server.router.security.token;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.router.security.RouterDTStateStoreUtils;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenIdentifier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ZKDelegationTokenSecretManagerImpl extends
        ZKDelegationTokenSecretManager<AbstractDelegationTokenIdentifier> {

    public static final String ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL =
            "zk-dt-secret-manager.router.token.sync.interval";
    public static final int ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL_DEFAULT = 5;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    // Local cache of delegation tokens, used for depracating tokens from
    // currentTokenMap
    private final Set<AbstractDelegationTokenIdentifier> localTokenCache =
            new HashSet<>();
    // Native zk client for getting all tokens
    private ZooKeeper zookeeper;
    private final String TOKEN_PATH = "/" + zkClient.getNamespace()
            + ZK_DTSM_TOKENS_ROOT;
    // The flag used to issue an extra check before deletion
    // Since cancel token and token remover thread use the same
    // API here and one router could have a token that is renewed
    // by another router, thus token remover should always check ZK
    // to confirm whether it has been renewed or not
    private ThreadLocal<Boolean> checkAgainstZkBeforeDeletion =
            new ThreadLocal<Boolean>() {
                @Override
                protected Boolean initialValue() {
                    return true;
                }
            };

    private static final Logger LOG =
            LoggerFactory.getLogger(ZKDelegationTokenSecretManagerImpl.class);
    private Configuration conf;

    public ZKDelegationTokenSecretManagerImpl(Configuration conf) {
        super(conf);
        LOG.info("Start zk backed secret manager instance");
        this.conf = conf;
        try {
            startThreads();
        } catch (IOException e) {
            LOG.error("Error starting threads for zkDelegationTokens ");
        }
    }

    @Override
    public void startThreads() throws IOException {
        super.startThreads();
        // start token cache related work when watcher is disabled
        if (!isTokenWatcherEnabled()) {
            LOG.info("Watcher for tokens is disabled in this secret manager");
            try {
                // By default set this variable
                checkAgainstZkBeforeDeletion.set(true);
                // Ensure the token root path exists
                if (zkClient.checkExists().forPath(ZK_DTSM_TOKENS_ROOT) == null) {
                    zkClient.create().creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(ZK_DTSM_TOKENS_ROOT);
                }
                // Set up zookeeper client
                try {
                    zookeeper = zkClient.getZookeeperClient().getZooKeeper();
                } catch (Exception e) {
                    LOG.info("Cannot get zookeeper client ", e);
                } finally {
                    if (zookeeper == null) {
                        throw new IOException("Zookeeper client is null");
                    }
                }

                LOG.info("Start loading token cache");
                long start = Time.now();
                rebuildTokenCache(true);
                LOG.info("Loaded token cache in {} milliseconds", Time.now() - start);

                int syncInterval = conf.getInt(ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL,
                        ZK_DTSM_ROUTER_TOKEN_SYNC_INTERVAL_DEFAULT);
                scheduler.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            rebuildTokenCache(false);
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }, syncInterval, syncInterval, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOG.error("Error rebuilding local cache for zkDelegationTokens ", e);
            }
        }
    }

    /**
     * This function will rebuild local token cache from zk storage.
     * It is first called when the secret manager is initialized and
     * then regularly at a configured interval.
     *
     * @param initial whether this is called during initialization
     * @throws IOException
     */
    private void rebuildTokenCache(boolean initial) throws IOException {
        localTokenCache.clear();
        // Use bare zookeeper client to get all children since curator will
        // wrap the same API with a sorting process. This is time consuming given
        // millions of tokens
        List<String> zkTokens;
        try {
            zkTokens = zookeeper.getChildren(TOKEN_PATH, false);
        } catch (KeeperException | InterruptedException e) {
            throw new IOException("Tokens cannot be fetched from path "
                    + TOKEN_PATH, e);
        }
        byte[] data;
        for (String tokenPath : zkTokens) {
            try {
                data = zkClient.getData().forPath(
                        ZK_DTSM_TOKENS_ROOT + "/" + tokenPath);
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("No node in path [" + tokenPath + "]");
                continue;
            } catch (Exception ex) {
                throw new IOException(ex);
            }
            // Store data to currentTokenMap
            processTokenAddOrUpdate(data);
            // Store data to localTokenCache for sync
            RouterDelegationTokenIdentifierData identifierData  =
                    RouterDTStateStoreUtils.getRouterDelegationTokenIdentifierData(data);
            RouterDelegationTokenIdentifier ident =
                    identifierData.getTokenIdentifier();
            localTokenCache.add(ident);
        }
        if (!initial) {
            // Sync zkTokens with local cache, specifically
            // 1) add/update tokens to local cache from zk, which is done through
            //    processTokenAddOrUpdate above
            // 2) remove tokens in local cache but not in zk anymore
            for (AbstractDelegationTokenIdentifier ident : currentTokens.keySet()) {
                if (!localTokenCache.contains(ident)) {
                    currentTokens.remove(ident);
                }
            }
        }
    }

    @Override
    protected void processTokenAddOrUpdate(byte[] data) throws IOException {
        RouterDelegationTokenIdentifierData identifierData  =
                RouterDTStateStoreUtils.getRouterDelegationTokenIdentifierData(data);
        RouterDelegationTokenIdentifier ident =
                identifierData.getTokenIdentifier();
        long renewDate = identifierData.getRenewDate();
        byte[] password = identifierData.getPassword();
        DelegationTokenInformation tokenInfo =
                new DelegationTokenInformation(renewDate, password);
        currentTokens.put(ident, tokenInfo);
    }

    protected void processTokenRemoved(ChildData data) throws IOException {
        RouterDelegationTokenIdentifierData identifierData  =
            RouterDTStateStoreUtils.getRouterDelegationTokenIdentifierData(data.getData());
        RouterDelegationTokenIdentifier ident =
            identifierData.getTokenIdentifier();
        currentTokens.remove(ident);
    }

    @Override
    protected void addOrUpdateToken(AbstractDelegationTokenIdentifier ident,
                                   DelegationTokenInformation info, boolean isUpdate) throws Exception {

        // Store the data in local memory first
        currentTokens.put(ident, info);
        String nodeCreatePath =
                getNodePath(ZK_DTSM_TOKENS_ROOT, DELEGATION_TOKEN_PREFIX
                        + ident.getSequenceNumber());

        long renewData = info.getRenewDate();
        RouterDelegationTokenIdentifierData identifierData =
                new RouterDelegationTokenIdentifierData((RouterDelegationTokenIdentifier) ident,
                        renewData, info.getPassword());
        try (ByteArrayOutputStream tokenOs = new ByteArrayOutputStream();
             DataOutputStream tokenOut = new DataOutputStream(tokenOs)) {
            identifierData.write(tokenOut);
            if (isUpdate) {
                zkClient.setData().forPath(nodeCreatePath, tokenOs.toByteArray())
                        .setVersion(-1);
            } else {
                zkClient.create().withMode(CreateMode.PERSISTENT)
                        .forPath(nodeCreatePath, tokenOs.toByteArray());
            }
        }
    }

    @Override
    protected DelegationTokenInformation getTokenInfoFromZK(String nodePath,
                                                            boolean quiet) throws IOException {
        try {
            byte[] data = zkClient.getData().forPath(nodePath);
            if ((data == null) || (data.length == 0)) {
                return null;
            }
            RouterDelegationTokenIdentifierData identifierData  =
                    RouterDTStateStoreUtils.getRouterDelegationTokenIdentifierData(data);
            long renewDate = identifierData.getRenewDate();
            byte[] password = identifierData.getPassword();
            DelegationTokenInformation tokenInfo =
                    new DelegationTokenInformation(renewDate, password);
            return tokenInfo;
        } catch (KeeperException.NoNodeException e) {
            if (!quiet) {
                LOG.error("No node in path [" + nodePath + "]");
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return null;
    }

    @Override
    public void stopThreads() {
        super.stopThreads();
        scheduler.shutdown();
    }

    @Override
    public AbstractDelegationTokenIdentifier createIdentifier() {
        return new RouterDelegationTokenIdentifier();
    }

    @Override
    public AbstractDelegationTokenIdentifier cancelToken(
            Token<AbstractDelegationTokenIdentifier> token, String canceller)
            throws IOException {
        checkAgainstZkBeforeDeletion.set(false);
        AbstractDelegationTokenIdentifier ident = super.cancelToken(token,
                canceller);
        checkAgainstZkBeforeDeletion.set(true);
        return ident;
    }

    @Override
    protected void removeStoredToken(AbstractDelegationTokenIdentifier ident)
            throws IOException {
        super.removeStoredToken(ident, checkAgainstZkBeforeDeletion.get());
    }

}
