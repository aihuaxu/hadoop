package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class GetPelotonNodeLabelResponsePBImpl extends GetPelotonNodeLabelResponse implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonNodeLabelResponseProto,
      YarnServerRouterProtos.GetPelotonNodeLabelResponseProto.Builder,
      YarnServerRouterProtos.GetPelotonNodeLabelResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonNodeLabelResponseProto,
          YarnServerRouterProtos.GetPelotonNodeLabelResponseProto.Builder,
          YarnServerRouterProtos.GetPelotonNodeLabelResponseProtoOrBuilder>(
          YarnServerRouterProtos.GetPelotonNodeLabelResponseProto.class);

  public GetPelotonNodeLabelResponsePBImpl() {}

  public GetPelotonNodeLabelResponsePBImpl(YarnServerRouterProtos.GetPelotonNodeLabelResponseProto proto) {
    setProto(proto);
  }
  @Override
  public YarnServerRouterProtos.GetPelotonNodeLabelResponseProto getProto() {
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

  @Override
  public String getPelotonNodeLabel() {
    return translator.getProtoOrBuilder().getLabel();
  }

  @Override
  public void setPelotonNodeLabel(String nodeLabel) {
    translator.getBuilder().setLabel(nodeLabel);
  }
}
