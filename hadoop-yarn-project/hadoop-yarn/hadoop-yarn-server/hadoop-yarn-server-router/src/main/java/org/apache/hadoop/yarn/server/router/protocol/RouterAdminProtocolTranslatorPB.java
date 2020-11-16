package org.apache.hadoop.yarn.server.router.protocol;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.ProtocolTranslator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.store.CachedRecordStore;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.ClearAllPelotonZKConfsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.SavePelotonZKConfRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.SavePelotonZKConfResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RemovePelotonZKInfoFromClusterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.GetPelotonZKConfListResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.GetPelotonZKConfListRequestProto;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonNodeLabelManager;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfManager;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
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
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.ClearAllPelotonZKConfsResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelResponsePBImpl;
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
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonZKConfListResponsePBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonNodeLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class RouterAdminProtocolTranslatorPB implements ProtocolMetaInterface, PelotonZKConfManager, PelotonNodeLabelManager, Closeable,
    ProtocolTranslator {
  final private RouterAdminProtocolPB rpcProxy;
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterAdminProtocolTranslatorPB.class);

  public RouterAdminProtocolTranslatorPB(RouterAdminProtocolPB proxy) {
    rpcProxy = proxy;
  }
  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        RouterAdminProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(RouterAdminProtocolPB.class), methodName);
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public GetPelotonZKConfListResponse getPelotonZKConfList(GetPelotonZKConfListRequest request) throws IOException {
    GetPelotonZKConfListRequestProto proto = GetPelotonZKConfListRequestProto.newBuilder().build();
    try {
      GetPelotonZKConfListResponseProto response = rpcProxy.getPelotonZKConfList(null, proto);
      return new GetPelotonZKConfListResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public SavePelotonZKConfResponse savePelotonZKConf(SavePelotonZKConfRequest request) throws IOException {
    SavePelotonZKConfRequestPBImpl requestPB = (SavePelotonZKConfRequestPBImpl)request;
    SavePelotonZKConfRequestProto proto = requestPB.getProto();
    try {
      SavePelotonZKConfResponseProto response = rpcProxy.savePelotonZKConf(null, proto);
      return new SavePelotonZKConfResponsePBImpl(response);
    } catch (Exception e) {
      throw new IOException("",e);
    }
  }

  @Override
  public RemovePelotonZKConfByClusterResponse removePelotonZKConfByCluster(RemovePelotonZKConfByClusterRequest request) throws IOException {
    RemovePelotonZKConfByClusterRequestPBImpl requestPB = (RemovePelotonZKConfByClusterRequestPBImpl)request;
    RemovePelotonZKConfByClusterRequestProto proto = requestPB.getProto();
    try {
      RemovePelotonZKConfByClusterResponseProto response = rpcProxy.removePelotonZKConfByCluster(null, proto);
      return new RemovePelotonZKConfByClusterResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public ClearAllPelotonZKConfsResponse clearAllPelotonZKConfs(ClearAllPelotonZKConfsRequest request) throws IOException {
    ClearAllPelotonZKConfsRequestProto proto = ClearAllPelotonZKConfsRequestProto.newBuilder().build();
    try {
      ClearAllPelotonZKConfsResponseProto response = rpcProxy.clearAllPelotonZKConfs(null, proto);
      return new ClearAllPelotonZKConfsResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public GetPelotonZKInfoListByClusterResponse getPelotonZKConfListByCluster(GetPelotonZKInfoListByClusterRequest request) throws IOException {
    GetPelotonZKInfoListByClusterRequestPBImpl requestPB = (GetPelotonZKInfoListByClusterRequestPBImpl) request;
    GetPelotonZKInfoListByClusterRequestProto proto = requestPB.getProto();
    try {
      GetPelotonZKInfoListByClusterResponseProto response = rpcProxy.getPelotonZKConfListByCluster(null, proto);
      return new GetPelotonZKInfoListByClusterResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public SavePelotonZKInfoToClusterResponse savePelotonZKInfoToCluster(SavePelotonZKInfoToClusterRequest request) throws IOException {
    SavePelotonZKInfoToClusterRequestPBImpl requestPB = (SavePelotonZKInfoToClusterRequestPBImpl)request;
    SavePelotonZKInfoToClusterRequestProto proto = requestPB.getProto();
    try {
      SavePelotonZKInfoToClusterResponseProto response = rpcProxy.savePelotonZKInfoToCluster(null, proto);
      return new SavePelotonZKInfoToClusterResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public RemovePelotonZKInfoFromClusterResponse removePelotonZKInfoFromCluster(RemovePelotonZKInfoFromClusterRequest request) throws IOException {
    RemovePelotonZKInfoFromClusterRequestPBImpl requestPB = (RemovePelotonZKInfoFromClusterRequestPBImpl)request;
    RemovePelotonZKInfoFromClusterRequestProto proto = requestPB.getProto();
    try {
      RemovePelotonZKInfoFromClusterResponseProto response = rpcProxy.removePelotonZKInfoFromCluster(null, proto);
      return new RemovePelotonZKInfoFromClusterResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }

  @Override
  public GetPelotonNodeLabelResponse getPelotonNodeLabel(GetPelotonNodeLabelRequest request) throws IOException {
    YarnServerRouterProtos.GetPelotonNodeLabelRequestProto proto = YarnServerRouterProtos
        .GetPelotonNodeLabelRequestProto.newBuilder().build();
    try {
      YarnServerRouterProtos.GetPelotonNodeLabelResponseProto response = rpcProxy.getPelotonNodeLabel(null, proto);
      return new GetPelotonNodeLabelResponsePBImpl(response);
    } catch (ServiceException e) {
      return null;
    }
  }

  @Override
  public SavePelotonNodeLabelResponse savePelotonNodeLabel(SavePelotonNodeLabelRequest request) throws IOException {
    SavePelotonNodeLabelRequestPBImpl requestPB = (SavePelotonNodeLabelRequestPBImpl)request;
    YarnServerRouterProtos.SavePelotonNodeLabelRequestProto proto = requestPB.getProto();
    try {
      YarnServerRouterProtos.SavePelotonNodeLabelResponseProto response = rpcProxy.savePelotonNodeLabel(null, proto);
      return new SavePelotonNodeLabelResponsePBImpl(response);
    } catch (ServiceException e) {
      throw new IOException(ProtobufHelper.getRemoteException(e).getMessage());
    }
  }
}

