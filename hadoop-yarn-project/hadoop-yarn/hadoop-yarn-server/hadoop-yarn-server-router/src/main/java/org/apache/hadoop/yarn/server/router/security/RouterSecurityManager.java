package org.apache.hadoop.yarn.server.router.security;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;

import static org.apache.hadoop.yarn.server.router.RouterServerUtil.logAuditEvent;

import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.EnumSet;

// this is borrowed from RouterSecurityManager from 3.1 in hdfs router.
public class RouterSecurityManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterSecurityManager.class);

  private static boolean alwaysUseDelegationTokensForTests = false;

  private AbstractDelegationTokenSecretManager<RouterDelegationTokenIdentifier> dtSecretManager = null;

  public RouterSecurityManager(Configuration conf) {
    this.dtSecretManager = newSecretManager(conf);
  }

  @VisibleForTesting
  public RouterSecurityManager(AbstractDelegationTokenSecretManager<RouterDelegationTokenIdentifier>
                                   dtSecretManager) {
    this.dtSecretManager = dtSecretManager;
  }

  public AbstractDelegationTokenSecretManager<RouterDelegationTokenIdentifier>
  getSecretManager() {
    return this.dtSecretManager;
  }

  public void stop() {
    LOG.info("Stopping security manager");
    if (this.dtSecretManager != null) {
      this.dtSecretManager.stopThreads();
    }
  }

  /**
   * Creates an instance of a SecretManager from the configuration.
   *
   * @param conf Configuration that defines the secret manager class.
   * @return New secret manager.
   */
  private static AbstractDelegationTokenSecretManager newSecretManager(
      Configuration conf) {

    Class<? extends AbstractDelegationTokenSecretManager> clazz = conf.getClass(
        RouterConfigKeys.ROUTER_SECRET_MANAGER_CLASS,
        RouterConfigKeys.ROUTER_SECRET_MANAGER_CLASS_DEFAULT,
        AbstractDelegationTokenSecretManager.class);
    AbstractDelegationTokenSecretManager secretManager = null;
    try {
      Constructor constructor =
          clazz.getConstructor(Configuration.class);
      secretManager = (AbstractDelegationTokenSecretManager) constructor.newInstance(conf);
      LOG.info("Secret manager instance created");
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(), e);
      return null;
    }
    return secretManager;
  }

  public Token<RouterDelegationTokenIdentifier> getDelegationToken(String renewer) throws IOException {
    LOG.debug("Generate delegation token with renewer " + renewer);
    final String operationName = "getDelegationToken";
    boolean success = false;
    String tokenId = "";
    Token<RouterDelegationTokenIdentifier> realRouterToken;
    try {
      // Verify that the connection is kerberos authenticated
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be issued only with kerberos authentication");
      }
      if (dtSecretManager == null || !dtSecretManager.isRunning()) {
        LOG.warn("trying to get DT with no secret manager running");
        return null;
      }
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      Text owner = new Text(ugi.getUserName());
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }

      // set token text to "" to skip token renewal in RM
      RouterDelegationTokenIdentifier tokenIdentifier =
          new RouterDelegationTokenIdentifier(owner, new Text(""),
              realUser);
      realRouterToken = new Token<RouterDelegationTokenIdentifier>(tokenIdentifier,
          dtSecretManager);
      return realRouterToken;
    } finally {
      logAuditEvent(success, operationName, tokenId);
    }
  }

  public long renewDelegationToken(Token<RouterDelegationTokenIdentifier> token)
      throws SecretManager.InvalidToken, IOException {
    if (isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be renewed only with kerberos authentication");
    }
    String user = getRenewerForToken(token);
    return dtSecretManager.renewToken(token, user);
  }

  public void cancelDelegationToken(Token<RouterDelegationTokenIdentifier> token)
      throws IOException {
    if (isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be renewed only with kerberos authentication");
    }
    String user = UserGroupInformation.getCurrentUser().getUserName();
    dtSecretManager.cancelToken(token, user);
  }

  private String getRenewerForToken(Token<RouterDelegationTokenIdentifier> token)
      throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    // we can always renew our own tokens
    return loginUser.getUserName().equals(user.getUserName())
        ? token.decodeIdentifier().getRenewer().toString()
        : user.getShortUserName();
  }

  /**
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    if (alwaysUseDelegationTokensForTests) return true;

    if (UserGroupInformation.isSecurityEnabled()) {
      return EnumSet.of(UserGroupInformation.AuthenticationMethod.KERBEROS,
          UserGroupInformation.AuthenticationMethod.KERBEROS_SSL,
          UserGroupInformation.AuthenticationMethod.CERTIFICATE)
          .contains(UserGroupInformation.getCurrentUser()
              .getRealAuthenticationMethod());
    } else {
      return true;
    }
  }
}
