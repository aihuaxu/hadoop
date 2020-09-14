package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.store.CachedRecordStore;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.Query;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PelotonZKConfRecordStore extends CachedRecordStore<PelotonZKConf> implements PelotonZKConfManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(PelotonZKConfRecordStore.class);
  public PelotonZKConfRecordStore(StateStoreDriver driver) {
    super(PelotonZKConf.class, driver);
  }

  @Override
  public GetPelotonZKConfListResponse getPelotonZKConfList(GetPelotonZKConfListRequest request) throws IOException {
    List<PelotonZKConf> targets = getCachedRecords();
    GetPelotonZKConfListResponse response = GetPelotonZKConfListResponse.newInstance();
    response.setPelotonZKConfList(targets);
    return response;
  }

  @Override
  public SavePelotonZKConfResponse savePelotonZKConf(SavePelotonZKConfRequest request) throws IOException {
    PelotonZKConf zkConf = request.getPelotonZKConf();
    boolean isCreate = request.getIsCreate();
    boolean status;
    if (isCreate) {
      status = getDriver().put(zkConf, false, true);
    } else {
      status = getDriver().put(zkConf, true, false);
    }
    SavePelotonZKConfResponse response = SavePelotonZKConfResponse.newInstance(status);
    if (status) {
      loadCache(true);
    }
    return response;
  }

  @Override
  public RemovePelotonZKConfByClusterResponse removePelotonZKConfByCluster(RemovePelotonZKConfByClusterRequest request) throws IOException {
    RemovePelotonZKConfByClusterResponse response = RemovePelotonZKConfByClusterResponse.newInstance();
    final String cluster = request.getCluster();
    final PelotonZKConf zkConf = PelotonZKConf.newInstance();
    zkConf.setCluster(cluster);
    final Query<PelotonZKConf> query = new Query<>(zkConf);
    final PelotonZKConf deleteTarget = getDriver().get(getRecordClass(), query);
    boolean status;
    String errorMessage = "";
    if (deleteTarget == null) {
      status = false;
      errorMessage = "target does not exist.";
    } else {
      status = getDriver().remove(deleteTarget);
      loadCache(true);
    }
    response.setStatus(status);
    response.setErrorMessage(errorMessage);
    return response;
  }

  @Override
  public ClearAllPelotonZKConfsResponse clearAllPelotonZKConfs(ClearAllPelotonZKConfsRequest request) throws IOException {
    ClearAllPelotonZKConfsResponse response = ClearAllPelotonZKConfsResponse.newInstance();
    boolean status = getDriver().removeAll(PelotonZKConf.class);
    response.setStatus(status);
    loadCache(true);
    return response;
  }

  @Override
  public GetPelotonZKInfoListByClusterResponse getPelotonZKConfListByCluster(
      GetPelotonZKInfoListByClusterRequest request) throws IOException {
    GetPelotonZKInfoListByClusterResponse response = GetPelotonZKInfoListByClusterResponse.newInstance();
    String cluster = request.getCluster();
    PelotonZKConf queryResult = getZKConfByClusterFromStore(cluster);
    List<PelotonZKInfo> zkInfoList = queryResult.getPelotonZKInfoList();
    response.setPelotonZKInfoList(zkInfoList);
    return response;
  }

  @Override
  public SavePelotonZKInfoToClusterResponse savePelotonZKInfoToCluster(SavePelotonZKInfoToClusterRequest request) throws IOException {
    SavePelotonZKInfoToClusterResponse response = SavePelotonZKInfoToClusterResponse.newInstance();
    String cluster = request.getCluster();
    PelotonZKConf queryResult = getZKConfByClusterFromStore(cluster);
    String errorMsg = "";
    boolean status;
    PelotonZKInfo zkInfo = request.getPelotonZkInfo();
    boolean isCreate = request.getIsCreate();
    List<PelotonZKInfo> zkInfoList = queryResult.getPelotonZKInfoList();
    if ((zkInfoList == null || zkInfoList.isEmpty())&& !isCreate) {
      response.setStatus(false);
      response.setErrorMessage("zk info list is empty");
      return response;
    }
    if (zkInfoList == null) {
      zkInfoList = new ArrayList<>();
    }
    int idx = zkInfoList.indexOf(zkInfo);
    if (idx <= -1 && !isCreate) {
      response.setStatus(false);
      response.setErrorMessage("zk address " + zkInfo.getZKAddress() + " in cluster" + cluster + " does not exist");
      return response;
    } else if (idx > -1 && isCreate) {
      status = false;
      errorMsg = "zk configuration with zk: " +zkInfo.getZKAddress() + " already exist";
    } else if (idx <= -1){
      zkInfoList.add(zkInfo);
      queryResult.setPelotonZKInfo(zkInfoList);
      status = getDriver().put(queryResult, true, false);
    } else {
      PelotonZKInfo updateTarget = zkInfoList.get(idx);
      updateTarget.setRegion(zkInfo.getRegion());
      updateTarget.setZone(zkInfo.getZone());
      queryResult.setPelotonZKInfo(zkInfoList);
      status = getDriver().put(queryResult, true, false);
    }
    response.setStatus(status);
    response.setErrorMessage(errorMsg);
    if (status) {
      loadCache(true);
    }
    return response;
  }

  @Override
  public RemovePelotonZKInfoFromClusterResponse removePelotonZKInfoFromCluster(RemovePelotonZKInfoFromClusterRequest request) throws IOException {
    RemovePelotonZKInfoFromClusterResponse response = RemovePelotonZKInfoFromClusterResponse.newInstance();
    String cluster = request.getCluster();
    PelotonZKConf queryResult = getZKConfByClusterFromStore(cluster);
    boolean status;
    List<PelotonZKInfo> zkInfoList = queryResult.getPelotonZKInfoList();
    if (zkInfoList == null || zkInfoList.isEmpty()) {
      response.setStatus(false);
      response.setErrorMessage("zk info list is empty");
      return response;
    }
    PelotonZKInfo zkInfo = PelotonZKInfo.newInstance(request.getZKAddress());
    int idx = zkInfoList.indexOf(zkInfo);
    if (idx > -1) {
      zkInfoList.remove(idx);
      queryResult.setPelotonZKInfo(zkInfoList);
      status = getDriver().put(queryResult, true, false);
      response.setStatus(status);
      if (status) {
        loadCache(true);
      }
      return response;
    } else {
      response.setStatus(false);
      response.setErrorMessage("zkAdress: " + request.getZKAddress() + " does not exist.");
      return response;
    }
  }

  @Override
  public String getRecordName(Class<? extends BaseRecord> cls) {
    return RouterServerUtil.getRecordName(cls);
  }

  private PelotonZKConf  getZKConfByClusterFromStore(String cluster) throws IOException {
    final PelotonZKConf targetZKConf = PelotonZKConf.newInstance();
    targetZKConf.setCluster(cluster);
    Query<PelotonZKConf> query = new Query<>(targetZKConf);
    PelotonZKConf queryResult = getDriver().get(getRecordClass(), query);
    if (queryResult == null) {
      throw new IOException("zk configuration with: " + targetZKConf + " does not exist in cluster: " + cluster);
    }
    return queryResult;
  }
}

