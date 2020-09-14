package org.apache.hadoop.yarn.server.router;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfManager;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.ClearAllPelotonZKConfsResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKConfListResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKInfoListByClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKConfByClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKConfResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertTrue;

public class TestRouterAdmin {
  private static Configuration conf;
  private static Router router;
  private static PelotonZKConfManager pelotonZKConfManager;

  @BeforeClass
  public static void create() throws Exception {
    // Basic configuration without the state store
    conf = new Configuration();
    conf.set(YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_KEY, "user@_HOST/REALM");
    conf.set(YarnConfiguration.ROUTER_KEYTAB_FILE_KEY, "/testfile");
    conf.set(YarnConfiguration.ROUTER_BIND_HOST, "0.0.0.0");
    conf.set(YarnConfiguration.ROUTER_CLIENTRM_ADDRESS, "testhost.com");
    conf.set(YarnConfiguration.ROUTER_RMADMIN_ADDRESS, "testhostadmin.com");

    router = new Router();
    Configuration routerConfig = new RouterConfigBuilder(conf).build();
    pelotonZKConfManager = mock(PelotonZKConfManager.class);
    router.init(routerConfig);
    router.start();
  }

  @AfterClass
  public static void stopRouter() {
    try {
      router.shutDown();

      int loopCount = 0;
      while (router.getServiceState() != Service.STATE.STOPPED) {
        loopCount++;
        Thread.sleep(1000);
        if (loopCount > 20) {
          break;
        }
      }
    } catch (InterruptedException e) {
    }
  }

  public List<PelotonZKConf> generateZKConf() {
    List<PelotonZKConf> zkConfs = new ArrayList<>();
    List<PelotonZKInfo> zkInfos = new ArrayList<>();
    zkInfos.add(PelotonZKInfo.newInstance("testZone", "testRegion", "testZK"));
    zkConfs.add(PelotonZKConf.newInstance("test", zkInfos));
    return zkConfs;
  }

  @Test
  public void testSavePelotonZKConf() throws IOException {
    SavePelotonZKConfResponse mockResponse = new SavePelotonZKConfResponsePBImpl();
    mockResponse.setStatus(true);
    when(pelotonZKConfManager.savePelotonZKConf(any(SavePelotonZKConfRequest.class))).thenReturn(mockResponse);
    SavePelotonZKConfResponse response = pelotonZKConfManager.savePelotonZKConf(
        SavePelotonZKConfRequest.newInstance());
    assertTrue(response.getStatus());
  }

  @Test
  public void testSavePelotonZKConfError() throws IOException {
    SavePelotonZKConfResponse mockResponse = new SavePelotonZKConfResponsePBImpl();
    mockResponse.setStatus(false);
    mockResponse.setErrorMessage("error");
    when(pelotonZKConfManager.savePelotonZKConf(any(SavePelotonZKConfRequest.class))).thenReturn(mockResponse);
    SavePelotonZKConfResponse response = pelotonZKConfManager.savePelotonZKConf(
        SavePelotonZKConfRequest.newInstance());
    assertFalse(response.getStatus());
    assertEquals("error", response.getErrorMessage());
  }

  @Test
  public void testClearAllPelotonZKConfs() throws IOException {
    ClearAllPelotonZKConfsResponse mockResponse = new ClearAllPelotonZKConfsResponsePBImpl();
    mockResponse.setStatus(true);
    when(pelotonZKConfManager.clearAllPelotonZKConfs(any(ClearAllPelotonZKConfsRequest.class))).thenReturn(mockResponse);
    ClearAllPelotonZKConfsResponse response = pelotonZKConfManager.clearAllPelotonZKConfs(ClearAllPelotonZKConfsRequest.newInstance());
    assertTrue(response.getStatus());
  }

  @Test
  public void testGetPelotonZKConfList() throws IOException {
    GetPelotonZKConfListResponse mockResponse = new GetPelotonZKConfListResponsePBImpl();
    List<PelotonZKConf> zkConfs = generateZKConf();
    mockResponse.setPelotonZKConfList(zkConfs);
    when(pelotonZKConfManager.getPelotonZKConfList(any(GetPelotonZKConfListRequest.class))).thenReturn(mockResponse);
    GetPelotonZKConfListResponse response = pelotonZKConfManager.getPelotonZKConfList(GetPelotonZKConfListRequest.newInstance());
    assertEquals(zkConfs.size(), response.getPelotonZKConfList().size());
  }

  @Test
  public void testGetPelotonZKInfoListByCluster() throws IOException {
    GetPelotonZKInfoListByClusterResponse mockResponse = new GetPelotonZKInfoListByClusterResponsePBImpl();
    List<PelotonZKConf> zkConfs = generateZKConf();
    List<PelotonZKInfo> zkInfos = zkConfs.get(0).getPelotonZKInfoList();
    mockResponse.setPelotonZKInfoList(zkInfos);
    when(pelotonZKConfManager.getPelotonZKConfListByCluster(any(GetPelotonZKInfoListByClusterRequest.class))).thenReturn(mockResponse);
    GetPelotonZKInfoListByClusterResponse response = pelotonZKConfManager.getPelotonZKConfListByCluster(GetPelotonZKInfoListByClusterRequest.newInstance());
    assertEquals(zkInfos.size(), response.getPelotonZKInfoList().size());
  }

  @Test
  public void testRemovePelotonZKConfByCluster() throws IOException {
    RemovePelotonZKConfByClusterResponse mockReponse = new RemovePelotonZKConfByClusterResponsePBImpl();
    mockReponse.setStatus(true);
    when(pelotonZKConfManager.removePelotonZKConfByCluster(any(RemovePelotonZKConfByClusterRequest.class))).thenReturn(mockReponse);
    RemovePelotonZKConfByClusterResponse response = pelotonZKConfManager.removePelotonZKConfByCluster(RemovePelotonZKConfByClusterRequest.newInstance());
    assertTrue(response.getStatus());
  }
}
