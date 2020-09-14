package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class RemovePelotonZKInfoFromClusterRequestPBImpl extends RemovePelotonZKInfoFromClusterRequest implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto,
      YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto.Builder,
      YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto,
                YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto.Builder,
                YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProtoOrBuilder>(
          YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto.class);

  public RemovePelotonZKInfoFromClusterRequestPBImpl() {}

  public RemovePelotonZKInfoFromClusterRequestPBImpl(YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto proto) {
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
  public void setZKAddress(String zkAddress) {
    translator.getBuilder().setZkAddress(zkAddress);
  }

  @Override
  public String getZKAddress() {
    return translator.getProtoOrBuilder().getZkAddress();
  }

  @Override
  public YarnServerRouterProtos.RemovePelotonZKInfoFromClusterRequestProto getProto() {
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
