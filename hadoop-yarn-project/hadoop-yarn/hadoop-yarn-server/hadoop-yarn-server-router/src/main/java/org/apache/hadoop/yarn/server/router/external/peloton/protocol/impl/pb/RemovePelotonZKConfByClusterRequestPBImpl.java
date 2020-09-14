package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class RemovePelotonZKConfByClusterRequestPBImpl extends RemovePelotonZKConfByClusterRequest implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto,
        YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto.Builder,
        YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto,
          YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto.Builder,
          YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProtoOrBuilder>(
          YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto.class);

  public RemovePelotonZKConfByClusterRequestPBImpl() {}
  public RemovePelotonZKConfByClusterRequestPBImpl(YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto proto) {
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
  public YarnServerRouterProtos.RemovePelotonZKConfByClusterRequestProto getProto() {
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
