package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class GetPelotonZKInfoListByClusterRequestPBImpl extends GetPelotonZKInfoListByClusterRequest implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto,
      YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto.Builder,
      YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto,
                YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto.Builder,
                YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProtoOrBuilder>(
          YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto.class);

  public GetPelotonZKInfoListByClusterRequestPBImpl() {}

  public GetPelotonZKInfoListByClusterRequestPBImpl(YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto proto) {
    setProto(proto);
  }

  @Override
  public void setCluster(String cluster) {
    translator.getBuilder().setCluster(cluster);
  }

  @Override
  public String getCluster() {
    return translator.getProtoOrBuilder().getCluster();
  }

  @Override
  public YarnServerRouterProtos.GetPelotonZKInfoListByClusterRequestProto getProto() {
    return translator.build();
  }

  @Override
  public void setProto(Message proto) {
    translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    translator.readInstance(base64String);
  }
}

