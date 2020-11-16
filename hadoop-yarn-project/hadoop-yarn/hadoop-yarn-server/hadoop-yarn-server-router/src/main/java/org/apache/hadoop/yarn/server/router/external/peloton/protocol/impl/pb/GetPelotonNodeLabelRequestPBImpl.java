package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GetPelotonNodeLabelRequestPBImpl extends GetPelotonNodeLabelRequest implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonNodeLabelRequestProto,
      YarnServerRouterProtos.GetPelotonNodeLabelRequestProto.Builder,
      YarnServerRouterProtos.GetPelotonNodeLabelRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonNodeLabelRequestProto,
          YarnServerRouterProtos.GetPelotonNodeLabelRequestProto.Builder,
          YarnServerRouterProtos.GetPelotonNodeLabelRequestProtoOrBuilder>(
          YarnServerRouterProtos.GetPelotonNodeLabelRequestProto.class);

  public GetPelotonNodeLabelRequestPBImpl() {}

  public GetPelotonNodeLabelRequestPBImpl(YarnServerRouterProtos.GetPelotonNodeLabelRequestProto proto) {
    setProto(proto);
  }

  @Override
  public YarnServerRouterProtos.GetPelotonNodeLabelRequestProto getProto() {
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
