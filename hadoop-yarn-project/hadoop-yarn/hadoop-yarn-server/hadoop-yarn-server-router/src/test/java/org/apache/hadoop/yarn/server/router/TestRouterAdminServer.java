package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonNodeLabelRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfRecordStore;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelResponsePBImpl;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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

  @Before
  public void setup() throws Exception {
    router = spy(new Router());
    configuration = new YarnConfiguration();
    pelotonNodeLabelRecordStore = mock(PelotonNodeLabelRecordStore.class);
    routerStateStoreService = spy(new RouterStateStoreService());
    Mockito.doNothing().when(router).startWepApp();
    when(router.getStateStoreService()).thenReturn(routerStateStoreService);
    when(routerStateStoreService.getRegisteredRecordStore(PelotonNodeLabelRecordStore.class)).thenReturn(pelotonNodeLabelRecordStore);
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
    SavePelotonNodeLabelResponse response = routerAdminServer.savePelotonNodeLabel(savePelotonNodeLabelRequest);
    verify(pelotonNodeLabelRecordStore, times(1)).savePelotonNodeLabel(savePelotonNodeLabelRequest);
  }

}
