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

package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.LogVerificationAppender;
import org.apache.log4j.Level;
import org.junit.Test;

import static org.apache.hadoop.hdfs.server.federation.router.Constants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

/**
 * Test functionality of {@link FairnessManager}, which manages allocation of
 *  handlers among name services through configured FairnessPolicyController.
 */
public class TestRouterFairnessManager {

  private static String nameServices =
          "ns1.nn1, ns1.nn2, ns2.nn1, ns2.nn2";

  @Test
  public void testHandlerAllocationEqualAssignment() throws IOException {
    FairnessManager fairnessManager = getFairnessManager(30);
    verifyHandlerAllocation(fairnessManager);
  }

  @Test
  public void testHandlerAllocationWithLeftOverHandler() throws IOException {
    FairnessManager fairnessManager = getFairnessManager(31);
    // One extra handler should be allocated to commons.
    assertTrue(fairnessManager.grantPermission(CONCURRENT_NS));
    verifyHandlerAllocation(fairnessManager);
  }

  @Test
  public void testHandlerAllocationPreconfigured() throws IOException {
    Configuration conf = generateBaseConf(40);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 30);
    FairnessManager fairnessManager = createFairnessManager(conf);

    int i = 0;
    // ns1 should have 30 permits allocated
    while (i < 30) {
      assertTrue(fairnessManager.grantPermission("ns1"));
      i++;
    }

    i = 0;
    // ns2 should have 5 permits.
    // concurrent should have 5 permits.
    while (i < 5) {
      assertTrue(fairnessManager.grantPermission("ns2"));
      assertTrue(fairnessManager.grantPermission(CONCURRENT_NS));
      i++;
    }

    assertFalse(fairnessManager.grantPermission("ns1"));
    assertFalse(fairnessManager.grantPermission("ns2"));
    assertFalse(fairnessManager.grantPermission(CONCURRENT_NS));
  }

  @Test
  public void testAllocationErrorWithZeroHandlers()
          throws Exception {
    Configuration conf = createConf(0);
    String errorMsg = "Available handlers 0 lower than min 1";
    verifyInstantiationError(conf, errorMsg);
  }

  @Test
  public void testAllocationErrorForLowDefaultHandlers()
          throws Exception {
    Configuration conf = createConf(1);
    String errorMsg = "Available handlers 0 lower than min 1";
    verifyInstantiationError(conf, errorMsg);
  }

  @Test
  public void testAllocationErrorForLowDefaultHandlersPerNS()
          throws Exception {
    Configuration conf = createConf(1);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "concurrent", 1);
    String errorMsg = "Available handlers 0 lower than min 1";
    verifyInstantiationError(conf, errorMsg);
  }

  @Test
  public void testAllocationErrorForLowPreconfiguredHandlers()
          throws Exception {
    Configuration conf = createConf(1);
    conf.setInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + "ns1", 2);
    String errorMsg = "Available handlers -1 lower than min 0";
    verifyInstantiationError(conf, errorMsg);
  }

  @Test
  public void testLimitMaxPolicyController() throws IOException {
    Configuration conf = createConf(4);
    conf.setStrings(DFS_ROUTER_POLICY_CONTROLLER_DRIVER_CLASS,
        "org.apache.hadoop.hdfs.server.federation.fairness.LimitMaxPolicyController");
    FairnessManager fm = new FairnessManager(conf);
    assertTrue(fm.grantPermission("ns1"));
    assertTrue(fm.grantPermission("ns1"));
    // The 3rd ns1 permit should fail as it exceeds total ns1 permits.
    assertFalse(fm.grantPermission("ns1"));
    assertTrue(fm.grantPermission("ns2"));
    assertTrue(fm.grantPermission("ns2"));
    // The first concurrent ns permit should fail as running out of global permits.
    assertFalse(fm.grantPermission("concurrent"));

    fm.releasePermission("ns1");
    // The 3rd ns2 permit should fail as it exceeds total ns1 permits.
    assertFalse(fm.grantPermission("ns2"));
    assertTrue(fm.grantPermission("concurrent"));
  }

  private void verifyInstantiationError(Configuration conf, String msg)
          throws Exception {

    // Attach log appender to verify log output
    final LogVerificationAppender appender =
            new LogVerificationAppender();
    final org.apache.log4j.Logger logger =
            org.apache.log4j.Logger.getRootLogger();
    logger.addAppender(appender);
    logger.setLevel(Level.ERROR);

    new FairnessManager(conf);
    assertEquals(1, appender.countLinesWithMessage(msg));
  }

  private FairnessManager getFairnessManager(int handlers)
          throws IOException {

    return createFairnessManager(generateBaseConf(handlers));
  }

  private FairnessManager createFairnessManager(Configuration conf)
          throws IOException {
    return new FairnessManager(conf);
  }

  private Configuration generateBaseConf(int handlers) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handlers);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, nameServices);
    return conf;
  }

  private void verifyHandlerAllocation(FairnessManager fairnessManager)
          throws IOException {

    int i = 0;
    while (i < 10) {
      assertTrue(fairnessManager.grantPermission("ns1"));
      assertTrue(fairnessManager.grantPermission("ns2"));
      assertTrue(fairnessManager.grantPermission(CONCURRENT_NS));
      i++;
    }
    assertFalse(fairnessManager.grantPermission("ns1"));
    assertFalse(fairnessManager.grantPermission("ns2"));
    assertFalse(fairnessManager.grantPermission(CONCURRENT_NS));

    fairnessManager.releasePermission("ns1");
    fairnessManager.releasePermission("ns2");
    fairnessManager.releasePermission(CONCURRENT_NS);

    assertTrue(fairnessManager.grantPermission("ns1"));
    assertTrue(fairnessManager.grantPermission("ns2"));
    assertTrue(fairnessManager.grantPermission(CONCURRENT_NS));
  }

  private Configuration createConf(int handlers) {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, handlers);
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, nameServices);
    return conf;
  }
}
