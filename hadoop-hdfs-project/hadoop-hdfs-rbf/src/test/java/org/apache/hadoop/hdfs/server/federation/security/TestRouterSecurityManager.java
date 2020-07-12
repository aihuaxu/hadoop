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

package org.apache.hadoop.hdfs.server.federation.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.router.security.RouterSecurityManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics2.util.Metrics2Util.NameValuePair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.security.token.delegation.TestDelegationToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.junit.Assert.assertEquals;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test functionalities of {@link RouterSecurityManager}, which manages
 * delegation tokens for router.
 */
public class TestRouterSecurityManager {

  private static final Logger LOG =
          LoggerFactory.getLogger(TestRouterSecurityManager.class);

  private static RouterSecurityManager securityManager = null;

  @BeforeClass
  public static void createMockSecretManager() throws IOException {
    AbstractDelegationTokenSecretManager<DelegationTokenIdentifier>
            testDelegationTokenSecretManager =
            new TestDelegationTokenSecretManager(100, 100, 100, 100);
    testDelegationTokenSecretManager.startThreads();
    securityManager = new RouterSecurityManager
            (testDelegationTokenSecretManager);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testDelegationTokens() throws IOException {
    String[] groupsForTesting = new String[1];
    groupsForTesting[0] = "router_group";
    UserGroupInformation.setLoginUser(UserGroupInformation
            .createUserForTesting("router", groupsForTesting));

    // Get a delegation token
    Token<DelegationTokenIdentifier> token =
            securityManager.getDelegationToken(new Text("some_renewer"));
    assertNotNull(token);

    // Renew the delegation token
    UserGroupInformation.setLoginUser(UserGroupInformation
            .createUserForTesting("some_renewer", groupsForTesting));
    long updatedExpirationTime = securityManager.renewDelegationToken(token);
    assertTrue(updatedExpirationTime >= token.decodeIdentifier().getMaxDate());

    // Cancel the delegation token
    securityManager.cancelDelegationToken(token);

    String exceptionCause = "Renewal request for unknown token";
    exceptionRule.expect(SecretManager.InvalidToken.class);
    exceptionRule.expectMessage(exceptionCause);

    // This throws an exception as token has been cancelled.
    securityManager.renewDelegationToken(token);
  }

  @Test
  public void testDelgationTokenTopOwners() throws Exception {
    UserGroupInformation.reset();
    List<NameValuePair> topOwners;

    UserGroupInformation user = UserGroupInformation
        .createUserForTesting("abc", new String[]{"router_group"});
    UserGroupInformation.setLoginUser(user);
    Token dt = securityManager.getDelegationToken(new Text("abc"));
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(1, topOwners.size());
    assertEquals("abc", topOwners.get(0).getName());
    assertEquals(1, topOwners.get(0).getValue());

    securityManager.renewDelegationToken(dt);
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(1, topOwners.size());
    assertEquals("abc", topOwners.get(0).getName());
    assertEquals(1, topOwners.get(0).getValue());

    securityManager.cancelDelegationToken(dt);
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(0, topOwners.size());


    // Use proxy user - the code should use the proxy user as the real owner
    UserGroupInformation routerUser =
        UserGroupInformation.createRemoteUser("router");
    UserGroupInformation proxyUser = UserGroupInformation
        .createProxyUserForTesting("abc",
            routerUser,
            new String[]{"router_group"});
    UserGroupInformation.setLoginUser(proxyUser);

    Token proxyDT = securityManager.getDelegationToken(new Text("router"));
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(1, topOwners.size());
    assertEquals("router", topOwners.get(0).getName());
    assertEquals(1, topOwners.get(0).getValue());

    // router to renew tokens
    UserGroupInformation.setLoginUser(routerUser);
    securityManager.renewDelegationToken(proxyDT);
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(1, topOwners.size());
    assertEquals("router", topOwners.get(0).getName());
    assertEquals(1, topOwners.get(0).getValue());

    securityManager.cancelDelegationToken(proxyDT);
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(0, topOwners.size());


    // check rank by more users
    securityManager.getDelegationToken(new Text("router"));
    securityManager.getDelegationToken(new Text("router"));
    UserGroupInformation.setLoginUser(user);
    securityManager.getDelegationToken(new Text("router"));
    topOwners = securityManager.getSecretManager().getTopTokenRealOwners(2);
    assertEquals(2, topOwners.size());
    assertEquals("router", topOwners.get(0).getName());
    assertEquals(2, topOwners.get(0).getValue());
    assertEquals("abc", topOwners.get(1).getName());
    assertEquals(1, topOwners.get(1).getValue());
  }

  public static class TestDelegationTokenSecretManager
          extends AbstractDelegationTokenSecretManager<DelegationTokenIdentifier> {

    public boolean isStoreNewMasterKeyCalled = false;
    public boolean isRemoveStoredMasterKeyCalled = false;
    public boolean isStoreNewTokenCalled = false;
    public boolean isRemoveStoredTokenCalled = false;
    public boolean isUpdateStoredTokenCalled = false;
    public TestDelegationTokenSecretManager(
            long delegationKeyUpdateInterval,
            long delegationTokenMaxLifetime,
            long delegationTokenRenewInterval,
            long delegationTokenRemoverScanInterval) {
      super(delegationKeyUpdateInterval, delegationTokenMaxLifetime,
              delegationTokenRenewInterval, delegationTokenRemoverScanInterval);
    }

    public TestDelegationTokenSecretManager(Configuration conf) {
      this(10000, 10000, 10000, 10000);
      try {
        super.startThreads();
      } catch (IOException e) {
        LOG.error("Error starting threads for zkDelegationTokens ");
      }
    }

    @Override
    public DelegationTokenIdentifier createIdentifier() {
      return new DelegationTokenIdentifier();
    }

    @Override
    protected byte[] createPassword(DelegationTokenIdentifier t) {
      return super.createPassword(t);
    }

    @Override
    protected void storeNewMasterKey(DelegationKey key) throws IOException {
      isStoreNewMasterKeyCalled = true;
      super.storeNewMasterKey(key);
    }

    @Override
    protected void removeStoredMasterKey(DelegationKey key) {
      isRemoveStoredMasterKeyCalled = true;
      Assert.assertFalse(key.equals(allKeys.get(currentId)));
    }

    @Override
    protected void storeNewToken(DelegationTokenIdentifier ident,
                                 long renewDate) throws IOException {
      super.storeNewToken(ident, renewDate);
      isStoreNewTokenCalled = true;
    }

    @Override
    protected void removeStoredToken(DelegationTokenIdentifier ident)
            throws IOException {
      super.removeStoredToken(ident);
      isRemoveStoredTokenCalled = true;
    }

    @Override
    protected void updateStoredToken(DelegationTokenIdentifier ident,
                                     long renewDate) throws IOException {
      super.updateStoredToken(ident, renewDate);
      isUpdateStoredTokenCalled = true;
    }

    public byte[] createPassword(
            TestDelegationToken.TestDelegationTokenIdentifier t,
            DelegationKey key) {
      return SecretManager.createPassword(t.getBytes(), key.getKey());
    }

    public Map<DelegationTokenIdentifier,
            DelegationTokenInformation> getAllTokens() {
      return currentTokens;
    }

    public DelegationKey getKey(
            TestDelegationToken.TestDelegationTokenIdentifier id) {
      return allKeys.get(id.getMasterKeyId());
    }
  }
}
