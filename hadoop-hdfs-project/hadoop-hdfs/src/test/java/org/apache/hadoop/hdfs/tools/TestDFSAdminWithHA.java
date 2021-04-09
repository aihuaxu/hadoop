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
package org.apache.hadoop.hdfs.tools;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDFSAdminWithHA {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private MiniQJMHACluster cluster;
  private Configuration conf;
  private DFSAdmin admin;
  private static final PrintStream oldOut = System.out;
  private static final PrintStream oldErr = System.err;

  private static final String NSID = "ns1";
  private static String newLine = System.getProperty("line.separator");

  private void assertOutputMatches(String string, int repeat) {
    String expected = "";
    for (int i = 0; i < repeat; ++i) {
      expected += string;
    }
    String errOutput = new String(err.toByteArray(), Charsets.UTF_8);
    String output = new String(out.toByteArray(), Charsets.UTF_8);

    if (!errOutput.matches(expected) && !output.matches(expected)) {
      fail("Expected output to match '" + expected +
          "' but err_output was:\n" + errOutput +
          "\n and output was: \n" + output);
    }

    out.reset();
    err.reset();
  }

  private void setHAConf(Configuration conf, String... nnAddrs) {
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        "hdfs://" + NSID);
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, NSID);
    conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, NSID);
    String[] nnNames = new String[nnAddrs.length];
    for (int i = 0; i < nnNames.length; ++i) {
      nnNames[i] = "nn" + (i + 1);
    }
    conf.set(DFSUtil.addKeySuffixes(
        DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX, NSID),
        Joiner.on(",").join(nnNames));
    conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, nnNames[0]);
    for (int i = 0; i < nnNames.length; ++i) {
      conf.set(DFSUtil.addKeySuffixes(
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, NSID, nnNames[i]),
          nnAddrs[i]);
    }
  }

  private void setUpHaCluster(boolean security) throws Exception {
    setUpHaCluster(security, 0, 0);
  }

  private void setUpHaCluster(boolean security, int numObservers,
          int numDataNodes) throws Exception {
    conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
        security);
    cluster = new MiniQJMHACluster.Builder(conf).setNumObservers(numObservers)
            .setNumDataNodes(numDataNodes).build();
    String[] addresses = new String[2 + numObservers];
    for (int i = 0; i < addresses.length; ++i) {
      addresses[i] = cluster.getDfsCluster().getNameNode(i).getHostAndPort();
    }
    setHAConf(conf, addresses);
    admin = new DFSAdmin();
    admin.setConf(conf);
    assertTrue(HAUtil.isHAEnabled(conf, "ns1"));

    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));
  }

  @After
  public void tearDown() throws Exception {
    try {
      System.out.flush();
      System.err.flush();
    } finally {
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    if (admin != null) {
      admin.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
    out.reset();
    err.reset();
  }

  @Test(timeout = 30000)
  public void testSetSafeMode() throws Exception {
    setUpHaCluster(false);
    // Enter safemode
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine, 2);

    // Get safemode
    exitCode = admin.run(new String[] {"-safemode", "get"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine, 2);

    // Leave safemode
    exitCode = admin.run(new String[] {"-safemode", "leave"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Safe mode is OFF in.*";
    assertOutputMatches(message + newLine, 2);

    // Get safemode
    exitCode = admin.run(new String[] {"-safemode", "get"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Safe mode is OFF in.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test(timeout = 60000)
  public void testMarkBadDataNodes() throws Exception {
    // set up an HA cluster with 2 DataNodes
    setUpHaCluster(false, 0, 2);

    for (int i = 0; i < 2; i++) {
      DatanodeManager datanodeManager = cluster.getDfsCluster().getNameNode(i)
              .getNamesystem().getBlockManager().getDatanodeManager();
      DatanodeDescriptor[] datanodes = datanodeManager.getDatanodes()
              .toArray(DatanodeDescriptor.EMPTY_ARRAY);
      Assert.assertEquals(2, datanodes.length);
      for (DatanodeInfo node : datanodes) {
        Assert.assertFalse(node.isMarkedBad());
      }
    }

    final DatanodeID[] datanodes = new DatanodeID[2];
    datanodes[0] = cluster.getDfsCluster().getDataNodes().get(0).getDatanodeId();
    datanodes[1] = cluster.getDfsCluster().getDataNodes().get(1).getDatanodeId();
    final String addr0 = String.format("%s:%d", datanodes[0].getIpAddr(),
            datanodes[0].getXferPort());
    final String addr1 = String.format("%s:%d", datanodes[1].getIpAddr(),
            datanodes[1].getXferPort());

    // mark both datanodes as bad
    int ret = admin.run(new String[]{"-markBadDataNodes", "-badNodes",
            addr0 + "," + addr1});
    Assert.assertEquals(0, ret);
    for (int i = 0; i < 2; i++) {
      DatanodeManager datanodeManager = cluster.getDfsCluster().getNameNode(i)
              .getNamesystem().getBlockManager().getDatanodeManager();
      TestDFSAdmin.checkDataNode(datanodeManager, datanodes,
              new boolean[]{true, true});
    }

    // list bad datanodes
    ret = admin.run(new String[]{"-listBadDataNodes"});
    Assert.assertEquals(0, ret);
    String output = new String(out.toByteArray(), Charsets.UTF_8);
    oldOut.println(output);
    Assert.assertTrue(output.contains("DataNodes marked as bad (2) in "
            + cluster.getDfsCluster().getNameNode(0).getNameNodeAddress()));
    Assert.assertTrue(output.contains("DataNodes marked as bad (2) in "
            + cluster.getDfsCluster().getNameNode(1).getNameNodeAddress()));

    // reset both datanodes to normal
    ret = admin.run(new String[]{"-markBadDataNodes", "-normalNodes",
            addr0 + "," + addr1});
    Assert.assertEquals(0, ret);
    for (int i = 0; i < 2; i++) {
      DatanodeManager datanodeManager = cluster.getDfsCluster().getNameNode(i)
              .getNamesystem().getBlockManager().getDatanodeManager();
      TestDFSAdmin.checkDataNode(datanodeManager, datanodes,
              new boolean[]{false, false});
    }

    // list bad datanodes
    ret = admin.run(new String[]{"-listBadDataNodes"});
    Assert.assertEquals(0, ret);
    output = new String(out.toByteArray(), Charsets.UTF_8);
    oldOut.println(output);
    Assert.assertTrue(output.contains("DataNodes marked as bad (0) in "
            + cluster.getDfsCluster().getNameNode(0).getNameNodeAddress()));
    Assert.assertTrue(output.contains("DataNodes marked as bad (0) in "
            + cluster.getDfsCluster().getNameNode(1).getNameNodeAddress()));
  }

  @Test (timeout = 30000)
  public void testSaveNamespace() throws Exception {
    setUpHaCluster(false);
    // Safe mode should be turned ON in order to create namespace image.
    int exitCode = admin.run(new String[] {"-safemode", "enter"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Safe mode is ON in.*";
    assertOutputMatches(message + newLine, 2);

    exitCode = admin.run(new String[] {"-saveNamespace"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "Save namespace successful for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRestoreFailedStorage() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-restoreFailedStorage", "check"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "restoreFailedStorage is set to false for.*";
    // Default is false
    assertOutputMatches(message + newLine, 2);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "true"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage is set to true for.*";
    assertOutputMatches(message + newLine, 2);

    exitCode = admin.run(new String[] {"-restoreFailedStorage", "false"});
    assertEquals(err.toString().trim(), 0, exitCode);
    message = "restoreFailedStorage is set to false for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRefreshNodes() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes successful for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRefreshNodesWithObserver() throws Exception {
    setUpHaCluster(false, 2, 0);
    int exitCode = admin.run(new String[] {"-refreshNodes"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh nodes successful for.*";
    assertOutputMatches(message + newLine, 4);
  }

  @Test (timeout = 30000)
  public void testSetBalancerBandwidth() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "10"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Balancer bandwidth is set to 10 for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testSetNegativeBalancerBandwidth() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-setBalancerBandwidth", "-10"});
    assertEquals("Negative bandwidth value must fail the command", -1, exitCode);
  }

  @Test (timeout = 30000)
  public void testMetaSave() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-metasave", "dfs.meta"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Created metasave file dfs.meta in the log directory"
        + " of namenode.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRefreshServiceAcl() throws Exception {
    setUpHaCluster(true);
    int exitCode = admin.run(new String[] {"-refreshServiceAcl"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh service acl successful for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRefreshUserToGroupsMappings() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshUserToGroupsMappings"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh user to groups mapping successful for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRefreshSuperUserGroupsConfiguration() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(
        new String[] {"-refreshSuperUserGroupsConfiguration"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh super user groups configuration successful for.*";
    assertOutputMatches(message + newLine, 2);
  }

  @Test (timeout = 30000)
  public void testRefreshCallQueue() throws Exception {
    setUpHaCluster(false);
    int exitCode = admin.run(new String[] {"-refreshCallQueue"});
    assertEquals(err.toString().trim(), 0, exitCode);
    String message = "Refresh call queue successful for.*";
    assertOutputMatches(message + newLine, 2);
  }
}
