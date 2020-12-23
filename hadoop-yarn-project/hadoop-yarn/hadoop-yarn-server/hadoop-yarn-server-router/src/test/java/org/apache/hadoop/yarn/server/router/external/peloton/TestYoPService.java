package org.apache.hadoop.yarn.server.router.external.peloton;

import com.uber.peloton.client.HostManager;
import com.uber.peloton.client.ResourceManager;
import com.uber.peloton.client.StatelessJobService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetOrderedHostsResponsePBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.IncludeExternalHostsResponsePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeResponsePBImpl;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKInfoListByClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(BlockJUnit4ClassRunner.class)
@PrepareForTest({PelotonHelper.class})
public class TestYoPService {
  RouterStateStoreService mockRouterStateStoreService;
  PelotonZKConfRecordStore mockPelotonZKConfRecordStore;
  PelotonNodeLabelRecordStore mockPelotonNodeLabelRecordStore;
  RouterClientRMService mockRouterClientRMService;
  ResourceManagerAdministrationProtocol mockRMAdminProxy;
  PelotonHelper pelotonHelper;
  Configuration conf;

  @Mock
  HostManager mockHostManager;
  @Mock
  ResourceManager mockResourceManager;
  @Mock
  StatelessJobService mockStatelessJobService;

  @Captor
  private ArgumentCaptor<ReplaceLabelsOnNodeRequest> captor;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "test-cluster");
    conf.set(YarnConfiguration.ROUTER_YARN_USER_ON_PELOTON_PATH,"./src/test/resources/test_creds_file");
    mockRouterClientRMService = mock(RouterClientRMService.class);
    List<String> orderedHost = new ArrayList<>();
    orderedHost.add("host1");
    GetOrderedHostsResponse getOrderedHostsResponse = new GetOrderedHostsResponsePBImpl();
    getOrderedHostsResponse.setOrderedHosts(orderedHost);
    when(mockRouterClientRMService.getOrderedHosts(any(GetOrderedHostsRequest.class))).thenReturn(getOrderedHostsResponse);
    mockPelotonZKConfRecordStore = mock(PelotonZKConfRecordStore.class);
    List<PelotonZKInfo> zkInfoList = new ArrayList<>();
    PelotonZKInfo zkInfo = PelotonZKInfo.newInstance("test-zkaddress");
    zkInfoList.add(zkInfo);
    GetPelotonZKInfoListByClusterResponse getPelotonZKInfoListByClusterResponse = new GetPelotonZKInfoListByClusterResponsePBImpl();
    getPelotonZKInfoListByClusterResponse.setPelotonZKInfoList(zkInfoList);
    when(mockPelotonZKConfRecordStore.getPelotonZKConfListByCluster(any(GetPelotonZKInfoListByClusterRequest.class))).thenReturn(getPelotonZKInfoListByClusterResponse);
    mockPelotonNodeLabelRecordStore = mock(PelotonNodeLabelRecordStore.class);
    GetPelotonNodeLabelResponse getPelotonNodeLabelResponse = new GetPelotonNodeLabelResponsePBImpl();
    getPelotonNodeLabelResponse.setPelotonNodeLabel("test-label");
    when(mockPelotonNodeLabelRecordStore.getPelotonNodeLabel(any(GetPelotonNodeLabelRequest.class))).thenReturn(getPelotonNodeLabelResponse);
    mockRouterStateStoreService = mock(RouterStateStoreService.class);
    when(mockRouterStateStoreService.getRegisteredRecordStore(PelotonNodeLabelRecordStore.class)).thenReturn(mockPelotonNodeLabelRecordStore);
    when(mockRouterStateStoreService.getRegisteredRecordStore(PelotonZKConfRecordStore.class)).thenReturn(mockPelotonZKConfRecordStore);
    when(mockRouterStateStoreService.isDriverReady()).thenReturn(true);
    pelotonHelper = new PelotonHelper(mockRouterStateStoreService, mockRouterClientRMService, mockRMAdminProxy);
    PowerMockito.whenNew(HostManager.class)
        .withArguments(Mockito.anyString(), Mockito.anyString())
        .thenReturn(mockHostManager);
    when(mockHostManager.blockingConnect()).thenReturn(null);
    PowerMockito.whenNew(ResourceManager.class)
        .withArguments(Mockito.anyString(), Mockito.anyString())
        .thenReturn(mockResourceManager);
    when(mockResourceManager.blockingConnect()).thenReturn(null);
    PowerMockito.whenNew(StatelessJobService.class)
        .withArguments(Mockito.anyString(), Mockito.anyString())
        .thenReturn(mockStatelessJobService);
    when(mockStatelessJobService.blockingConnect()).thenReturn(null);
    pelotonHelper.enableTest();
  }

  @Test
  public void testGetOrderedHostsFromRM() {
    List<String> acutalOrderedHosts = pelotonHelper.getOrderedHostsListFromRM();
    assertEquals(1, acutalOrderedHosts.size());
    assertEquals("host1",acutalOrderedHosts.get(0));
  }

  @Test
  public void testInitialization() {
    pelotonHelper.initialize(conf);
    assertTrue(pelotonHelper.isInitialized());
  }

  @Test
  public void testConnectPelotonServices() throws Exception {
    if (!pelotonHelper.isInitialized()) {
      pelotonHelper.initialize(conf);
    }
    PelotonClientWrapper wrapper = pelotonHelper.getClientWrapper().get(0);
    assertEquals(mockHostManager,wrapper.getHostManagerClient());
    assertEquals(mockResourceManager,wrapper.getResourceManagerClient());
    assertEquals(mockStatelessJobService, wrapper.getStatelessSvcClient());
    pelotonHelper.connectPelotonServices();
    verify(mockHostManager, times(1)).blockingConnect();
    verify(mockResourceManager, times(1)).blockingConnect();
    verify(mockStatelessJobService, times(1)).blockingConnect();
  }

  @Test
  public void testStartNMOnPeloton() throws IOException, YarnException {
    IncludeExternalHostsResponse includeExternalHostsResponse = new IncludeExternalHostsResponsePBImpl();
    when(mockRouterClientRMService.includeExternalHosts(any(IncludeExternalHostsRequest.class))).thenReturn(includeExternalHostsResponse);
    mockRMAdminProxy = mock(ResourceManagerAdministrationProtocol.class);
    ReplaceLabelsOnNodeResponse replaceLabelsOnNodeResponse = new ReplaceLabelsOnNodeResponsePBImpl();
    when(mockRMAdminProxy.replaceLabelsOnNode(any(ReplaceLabelsOnNodeRequest.class))).thenReturn(replaceLabelsOnNodeResponse);
    pelotonHelper.startNMsOnPeloton();
  }
}
