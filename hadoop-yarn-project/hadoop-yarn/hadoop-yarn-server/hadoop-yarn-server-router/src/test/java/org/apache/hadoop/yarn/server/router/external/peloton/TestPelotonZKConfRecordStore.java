package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.Query;
import org.apache.hadoop.store.record.QueryResult;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.ClearAllPelotonZKConfsRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKConfListRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKInfoListByClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKConfByClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKInfoFromClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKConfRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKInfoToClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPelotonZKConfRecordStore {
  private StateStoreDriver stateStoreDriver;
  private PelotonZKConfRecordStore pelotonZKConfRecordStore;
  List<PelotonZKConf> records = new ArrayList<>();
  List<PelotonZKInfo> zkInfos = new ArrayList<>();
  private String ZK_ADDRESS = "zk-address";
  private String CLUSTER = "test-cluster";

  @Before
  public void setup() throws IOException {
    stateStoreDriver = mock(StateStoreDriver.class);
    zkInfos.add(PelotonZKInfo.newInstance(ZK_ADDRESS));
    records.add(PelotonZKConf.newInstance(CLUSTER, zkInfos));
    QueryResult<PelotonZKConf> queryResult = new QueryResult<>(records, 1);
    when(stateStoreDriver.get(PelotonZKConf.class)).thenReturn(queryResult);
    pelotonZKConfRecordStore = new PelotonZKConfRecordStore(stateStoreDriver);
  }

  @Test
  public void testGetPelotonZKConf() throws Exception {
    GetPelotonZKConfListRequest request = new GetPelotonZKConfListRequestPBImpl();
    pelotonZKConfRecordStore.loadCache(true);
    GetPelotonZKConfListResponse response = pelotonZKConfRecordStore.getPelotonZKConfList(request);
    Assert.assertEquals(1, response.getPelotonZKConfList().size());
  }

  @Test
  public void testSavePelotonZKConf() throws Exception {
    SavePelotonZKConfRequest request = new SavePelotonZKConfRequestPBImpl();
    request.setIsCreate(true);
    request.setPelotonZKConf(records.get(0));
    pelotonZKConfRecordStore.loadCache(true);
    SavePelotonZKConfResponse response = pelotonZKConfRecordStore.savePelotonZKConf(request);
    verify(stateStoreDriver, times(1)).put(request.getPelotonZKConf(), false, true);
  }

  @Test
  public void testRemovePelotonZKConfByCluster() throws Exception {
    RemovePelotonZKConfByClusterRequest request = new RemovePelotonZKConfByClusterRequestPBImpl();
    request.setCluster(CLUSTER);
    PelotonZKConf zkConf = PelotonZKConf.newInstance();
    zkConf.setCluster(CLUSTER);
    Query<PelotonZKConf> query = new Query<>(zkConf);
    pelotonZKConfRecordStore.loadCache(true);
    when(stateStoreDriver.get(PelotonZKConf.class, query)).thenReturn(records.get(0));
    pelotonZKConfRecordStore.removePelotonZKConfByCluster(request);
  }

  @Test
  public void testClearAllPelotonZKConfs() throws Exception {
    ClearAllPelotonZKConfsRequest request = new ClearAllPelotonZKConfsRequestPBImpl();
    when(stateStoreDriver.removeAll(PelotonZKConf.class)).thenReturn(true);
    ClearAllPelotonZKConfsResponse response = pelotonZKConfRecordStore.clearAllPelotonZKConfs(request);
    Assert.assertTrue(response.getStatus());
  }

  @Test
  public void testGetPelotonZKConfListByCluster() throws Exception {
    GetPelotonZKInfoListByClusterRequest request = new GetPelotonZKInfoListByClusterRequestPBImpl();
    request.setCluster(CLUSTER);
    when(stateStoreDriver.get(any(Class.class), any(Query.class))).thenReturn(records.get(0));
    GetPelotonZKInfoListByClusterResponse response = pelotonZKConfRecordStore.getPelotonZKConfListByCluster(request);
    Assert.assertEquals(1, response.getPelotonZKInfoList().size());
    Assert.assertEquals(ZK_ADDRESS, response.getPelotonZKInfoList().get(0).getZKAddress());
  }

  @Test
  public void testSavePelotonZKInfoToClusterAlreadyExists() throws Exception {
    SavePelotonZKInfoToClusterRequest request = new SavePelotonZKInfoToClusterRequestPBImpl();
    request.setCluster(CLUSTER);
    request.setPelotonInfo(zkInfos.get(0));
    request.setIsCreateNew(true);
    when(stateStoreDriver.get(any(Class.class), any(Query.class))).thenReturn(records.get(0));
    SavePelotonZKInfoToClusterResponse response = pelotonZKConfRecordStore.savePelotonZKInfoToCluster(request);
    Assert.assertFalse(response.getStatus());
    Assert.assertEquals("zk configuration with zk: zk-address already exist", response.getErrorMessage());
  }

  @Test
  public void testSavePelotonZKInfoToClusterCreateSucceed() throws Exception {
    SavePelotonZKInfoToClusterRequest request = new SavePelotonZKInfoToClusterRequestPBImpl();
    request.setCluster(CLUSTER);
    request.setPelotonInfo(PelotonZKInfo.newInstance(ZK_ADDRESS + "1"));
    request.setIsCreateNew(true);
    when(stateStoreDriver.get(any(Class.class), any(Query.class))).thenReturn(records.get(0));
    when(stateStoreDriver.put(any(PelotonZKConf.class), anyBoolean(), anyBoolean())).thenReturn(true);
    SavePelotonZKInfoToClusterResponse response = pelotonZKConfRecordStore.savePelotonZKInfoToCluster(request);
    Assert.assertTrue(response.getStatus());
  }

  @Test
  public void testRemovePelotonZKInfoFromClusterWithEmptyZKInfo() throws Exception {
    RemovePelotonZKInfoFromClusterRequest request = new RemovePelotonZKInfoFromClusterRequestPBImpl();
    request.setCluster(CLUSTER);
    PelotonZKConf zkConf = PelotonZKConf.newInstance();
    zkConf.setCluster(CLUSTER);
    when(stateStoreDriver.get(any(Class.class), any(Query.class))).thenReturn(zkConf);
    RemovePelotonZKInfoFromClusterResponse response = pelotonZKConfRecordStore.removePelotonZKInfoFromCluster(request);
    Assert.assertFalse(response.getStatus());
    Assert.assertEquals("zk info list is empty", response.getErrorMessage());
  }

  @Test
  public void testRemovePelotonZKInfoFromClusterWithZKInfo() throws Exception {
    RemovePelotonZKInfoFromClusterRequest request = new RemovePelotonZKInfoFromClusterRequestPBImpl();
    request.setCluster(CLUSTER);
    request.setZKAddress(ZK_ADDRESS);
    when(stateStoreDriver.put(any(PelotonZKConf.class), anyBoolean(), anyBoolean())).thenReturn(true);
    when(stateStoreDriver.get(any(Class.class), any(Query.class))).thenReturn(records.get(0));
    RemovePelotonZKInfoFromClusterResponse response = pelotonZKConfRecordStore.removePelotonZKInfoFromCluster(request);
    Assert.assertTrue(response.getStatus());
  }

  @Test
  public void testGetRecordName() {
    Assert.assertEquals("PelotonZKConf", pelotonZKConfRecordStore.getRecordName(PelotonZKConf.class));
  }
}
