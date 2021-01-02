package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonNodeLabelRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.ClearAllPelotonZKConfsRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.ClearAllPelotonZKConfsResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKConfListRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKConfListResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKInfoListByClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKInfoListByClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKInfoFromClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKInfoFromClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKConfRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKConfResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKInfoToClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKInfoToClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRouterAdminServer {
  private YarnConfiguration configuration;
  private Router router;
  private RouterStateStoreService routerStateStoreService;
  private PelotonZKConfRecordStore pelotonZKConfRecordStore;
  private PelotonNodeLabelRecordStore pelotonNodeLabelRecordStore;
  private RouterAdminServer routerAdminServer;
  List<PelotonZKConf> records = new ArrayList<>();
  List<PelotonZKInfo> zkInfos = new ArrayList<>();
  private String ZK_ADDRESS = "zk-address";
  private String CLUSTER = "test-cluster";

  @Before
  public void setup() throws Exception {
    router = spy(new Router());
    configuration = new YarnConfiguration();
    pelotonNodeLabelRecordStore = mock(PelotonNodeLabelRecordStore.class);
    pelotonZKConfRecordStore = mock(PelotonZKConfRecordStore.class);
    zkInfos.add(PelotonZKInfo.newInstance(ZK_ADDRESS));
    records.add(PelotonZKConf.newInstance(CLUSTER, zkInfos));
    routerStateStoreService = spy(new RouterStateStoreService());
    Mockito.doNothing().when(router).startWepApp();
    when(router.getStateStoreService()).thenReturn(routerStateStoreService);
    when(routerStateStoreService.getRegisteredRecordStore(PelotonNodeLabelRecordStore.class)).thenReturn(pelotonNodeLabelRecordStore);
    when(routerStateStoreService.getRegisteredRecordStore(PelotonZKConfRecordStore.class)).thenReturn(pelotonZKConfRecordStore);
    routerAdminServer = new RouterAdminServer(configuration, router);
  }

  @Test
  public void testRouterAdminServerStart() throws Exception {
    routerAdminServer.serviceInit(configuration);
    Assert.assertNotNull(this.routerAdminServer);
    routerAdminServer.serviceStart();
    Assert.assertEquals("/0.0.0.0:8111",routerAdminServer.getRpcAddress().toString());
  }

  @Test
  public void testGetPelotonNodeLabel() throws Exception {
    String TARGET_NODE_LABEL = "test-label";
    GetPelotonNodeLabelRequest getPelotonNodeLabelRequest = new GetPelotonNodeLabelRequestPBImpl();
    GetPelotonNodeLabelResponse mockGetPelotonNodeLabelResponse = new GetPelotonNodeLabelResponsePBImpl();
    mockGetPelotonNodeLabelResponse.setPelotonNodeLabel(TARGET_NODE_LABEL);
    when(pelotonNodeLabelRecordStore.getPelotonNodeLabel(getPelotonNodeLabelRequest)).thenReturn(mockGetPelotonNodeLabelResponse);
    GetPelotonNodeLabelResponse response = routerAdminServer.getPelotonNodeLabel(getPelotonNodeLabelRequest);
    Assert.assertEquals(TARGET_NODE_LABEL, response.getPelotonNodeLabel());
    verify(pelotonNodeLabelRecordStore, times(1)).getPelotonNodeLabel(getPelotonNodeLabelRequest);
  }

  @Test
  public void testSavePelotonNodeLabel() throws Exception {
    SavePelotonNodeLabelRequest savePelotonNodeLabelRequest = new SavePelotonNodeLabelRequestPBImpl();
    SavePelotonNodeLabelResponse mockSavePelotonNodeLabelResponse = new SavePelotonNodeLabelResponsePBImpl();
    when(pelotonNodeLabelRecordStore.savePelotonNodeLabel(savePelotonNodeLabelRequest)).thenReturn(mockSavePelotonNodeLabelResponse);
    routerAdminServer.savePelotonNodeLabel(savePelotonNodeLabelRequest);
    verify(pelotonNodeLabelRecordStore, times(1)).savePelotonNodeLabel(savePelotonNodeLabelRequest);
  }

  @Test
  public void testGetPelotonZKConfList() throws Exception {
    GetPelotonZKConfListRequest request = new GetPelotonZKConfListRequestPBImpl();
    GetPelotonZKConfListResponse mockResponse = new GetPelotonZKConfListResponsePBImpl();
    mockResponse.setPelotonZKConfList(records);
    when(pelotonZKConfRecordStore.getPelotonZKConfList(request)).thenReturn(mockResponse);
    Assert.assertEquals(1, routerAdminServer.getPelotonZKConfList(request).getPelotonZKConfList().size());
  }

  @Test
  public void testSavePelotonZKConf() throws Exception {
    SavePelotonZKConfRequest request = new SavePelotonZKConfRequestPBImpl();
    request.setPelotonZKConf(records.get(0));
    SavePelotonZKConfResponse mockResponse = new SavePelotonZKConfResponsePBImpl();
    when(pelotonZKConfRecordStore.savePelotonZKConf(request)).thenReturn(mockResponse);
    routerAdminServer.savePelotonZKConf(request);
    verify(pelotonZKConfRecordStore, times(1)).savePelotonZKConf(request);
  }

  @Test
  public void testClearAllPelotonZKConfs() throws Exception {
    ClearAllPelotonZKConfsRequest request = new ClearAllPelotonZKConfsRequestPBImpl();
    ClearAllPelotonZKConfsResponse mockResponse = new ClearAllPelotonZKConfsResponsePBImpl();
    when(pelotonZKConfRecordStore.clearAllPelotonZKConfs(request)).thenReturn(mockResponse);
    routerAdminServer.clearAllPelotonZKConfs(request);
    verify(pelotonZKConfRecordStore, times(1)).clearAllPelotonZKConfs(request);
  }

  @Test
  public void testGetPelotonZKConfListByCluster() throws Exception {
    GetPelotonZKInfoListByClusterRequest request = new GetPelotonZKInfoListByClusterRequestPBImpl();
    GetPelotonZKInfoListByClusterResponse mockResponse = new GetPelotonZKInfoListByClusterResponsePBImpl();
    mockResponse.setPelotonZKInfoList(zkInfos);
    when(pelotonZKConfRecordStore.getPelotonZKConfListByCluster(request)).thenReturn(mockResponse);
    Assert.assertEquals(1, routerAdminServer.getPelotonZKConfListByCluster(request).getPelotonZKInfoList().size());
  }

  @Test
  public void testSavePelotonZKInfoToCluster() throws Exception {
    SavePelotonZKInfoToClusterRequest request = new SavePelotonZKInfoToClusterRequestPBImpl();
    SavePelotonZKInfoToClusterResponse mockResponse = new SavePelotonZKInfoToClusterResponsePBImpl();
    when(pelotonZKConfRecordStore.savePelotonZKInfoToCluster(request)).thenReturn(mockResponse);
    routerAdminServer.savePelotonZKInfoToCluster(request);
    verify(pelotonZKConfRecordStore, times(1)).savePelotonZKInfoToCluster(request);
  }

  @Test
  public void testRemovePelotonZKInfoFromCluster() throws Exception {
    RemovePelotonZKInfoFromClusterRequest request = new RemovePelotonZKInfoFromClusterRequestPBImpl();
    RemovePelotonZKInfoFromClusterResponse mockResponse = new RemovePelotonZKInfoFromClusterResponsePBImpl();
    when(pelotonZKConfRecordStore.removePelotonZKInfoFromCluster(request)).thenReturn(mockResponse);
    routerAdminServer.removePelotonZKInfoFromCluster(request);
    verify(pelotonZKConfRecordStore, times(1)).removePelotonZKInfoFromCluster(request);
  }

}
