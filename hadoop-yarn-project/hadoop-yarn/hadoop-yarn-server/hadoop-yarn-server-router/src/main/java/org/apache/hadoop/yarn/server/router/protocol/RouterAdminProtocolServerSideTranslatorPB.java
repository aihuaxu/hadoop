package org.apache.hadoop.yarn.server.router.protocol;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.RouterAdminServer;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
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
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKConfByClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKConfByClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKInfoFromClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.RemovePelotonZKInfoFromClusterResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKConfRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKConfResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKInfoToClusterRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonZKInfoToClusterResponsePBImpl;

import java.io.IOException;


public class RouterAdminProtocolServerSideTranslatorPB implements RouterAdminProtocolPB{

  private final RouterAdminServer server;

  public RouterAdminProtocolServerSideTranslatorPB(RouterAdminServer server) throws IOException {
    this.server = server;
  }

  @Override
  public YarnServerRouterProtos.GetPelotonZKConfListResponseProto getPelotonZKConfList(RpcController controller,
      YarnServerRouterProtos.GetPelotonZKConfListRequestProto request) throws ServiceException {
    try {
      GetPelotonZKConfListRequest proto = new GetPelotonZKConfListRequestPBImpl(request);
      GetPelotonZKConfListResponse response = server.getPelotonZKConfList(proto);
      GetPelotonZKConfListResponsePBImpl responsePB = (GetPelotonZKConfListResponsePBImpl)response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed getPelotonZKConfList", e);
    }
  }

  @Override
  public YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto savePelotonZKInfoToCluster(
      RpcController controller, YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto request)
      throws ServiceException {
    try {
      SavePelotonZKInfoToClusterRequest req = new SavePelotonZKInfoToClusterRequestPBImpl(request);
      SavePelotonZKInfoToClusterResponse response = server.savePelotonZKInfoToCluster(req);
      SavePelotonZKInfoToClusterResponsePBImpl responsePB = (SavePelotonZKInfoToClusterResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed savePelotonZKInfoToCluster", e);
    }
  }

  @Override
  public YarnServerRouterProtos.RemovePelotonZKInfoFromClusterResponseProto removePelotonZKInfoFromCluster(RpcController controller,
      YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto request) throws ServiceException {
    try {
      RemovePelotonZKInfoFromClusterRequest req = new RemovePelotonZKInfoFromClusterRequestPBImpl(request);
      RemovePelotonZKInfoFromClusterResponse response = server.removePelotonZKInfoFromCluster(req);
      RemovePelotonZKInfoFromClusterResponsePBImpl responsePB = (RemovePelotonZKInfoFromClusterResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed removePelotonZKInfoFromCluster", e);
    }
  }

  @Override
  public YarnServerRouterProtos.SavePelotonZKConfResponseProto savePelotonZKConf(RpcController controller, YarnServerRouterProtos.SavePelotonZKConfRequestProto request) throws ServiceException {
    try {
      SavePelotonZKConfRequest req = new SavePelotonZKConfRequestPBImpl(request);
      SavePelotonZKConfResponse response = server.savePelotonZKConf(req);
      SavePelotonZKConfResponsePBImpl responsePB = (SavePelotonZKConfResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed savePelotonZKConf", e);
    }
  }

  @Override
  public YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto removePelotonZKConfByCluster(RpcController controller,
      YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto request) throws ServiceException {
    try {
      RemovePelotonZKConfByClusterRequest req = new RemovePelotonZKConfByClusterRequestPBImpl(request);
      RemovePelotonZKConfByClusterResponse response = server.removePelotonZKConfByCluster(req);
      RemovePelotonZKConfByClusterResponsePBImpl responsePB = (RemovePelotonZKConfByClusterResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed removePelotonZKConfByCluster", e);
    }
  }

  @Override
  public YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto getPelotonZKConfListByCluster(RpcController controller,
      YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto request) throws ServiceException {
    try {
      GetPelotonZKInfoListByClusterRequest req = new GetPelotonZKInfoListByClusterRequestPBImpl(request);
      GetPelotonZKInfoListByClusterResponse response = server.getPelotonZKConfListByCluster(req);
      GetPelotonZKInfoListByClusterResponsePBImpl responsePB = (GetPelotonZKInfoListByClusterResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed getPelotonZKConfListByCluster", e);
    }
  }

  @Override
  public YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto clearAllPelotonZKConfs(RpcController controller, YarnServerRouterProtos.ClearAllPelotonZKConfsRequestProto request) throws ServiceException {
    try {
      ClearAllPelotonZKConfsRequest req = new ClearAllPelotonZKConfsRequestPBImpl(request);
      ClearAllPelotonZKConfsResponse response = server.clearAllPelotonZKConfs(req);
      ClearAllPelotonZKConfsResponsePBImpl responsePB = (ClearAllPelotonZKConfsResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed clearAllPelotonZKConfs", e);
    }
  }

  @Override
  public YarnServerRouterProtos.GetPelotonNodeLabelResponseProto getPelotonNodeLabel(RpcController controller, YarnServerRouterProtos.GetPelotonNodeLabelRequestProto request) throws ServiceException {
    try {
      GetPelotonNodeLabelRequest req = new GetPelotonNodeLabelRequestPBImpl(request);
      GetPelotonNodeLabelResponse response = server.getPelotonNodeLabel(req);
      GetPelotonNodeLabelResponsePBImpl responsePB = (GetPelotonNodeLabelResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed getPelotonNodeLabel", e);
    }
  }

  @Override
  public YarnServerRouterProtos.SavePelotonNodeLabelResponseProto savePelotonNodeLabel(RpcController controller, YarnServerRouterProtos.SavePelotonNodeLabelRequestProto request) throws ServiceException {
    try {
      SavePelotonNodeLabelRequest req = new SavePelotonNodeLabelRequestPBImpl(request);
      SavePelotonNodeLabelResponse response = server.savePelotonNodeLabel(req);
      SavePelotonNodeLabelResponsePBImpl responsePB = (SavePelotonNodeLabelResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException("failed savePelotonNodeLabel", e);
    }
  }
}

