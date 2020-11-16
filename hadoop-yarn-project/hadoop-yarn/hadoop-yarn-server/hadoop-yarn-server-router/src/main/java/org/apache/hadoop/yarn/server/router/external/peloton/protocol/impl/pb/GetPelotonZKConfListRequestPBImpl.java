package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GetPelotonZKConfListRequestPBImpl extends GetPelotonZKConfListRequest implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKConfListRequestProto,
        YarnServerRouterProtos.GetPelotonZKConfListRequestProto.Builder,
        YarnServerRouterProtos.GetPelotonZKConfListRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKConfListRequestProto,
          YarnServerRouterProtos.GetPelotonZKConfListRequestProto.Builder,
          YarnServerRouterProtos.GetPelotonZKConfListRequestProtoOrBuilder>(
          YarnServerRouterProtos.GetPelotonZKConfListRequestProto.class);

  public GetPelotonZKConfListRequestPBImpl() {}

  public GetPelotonZKConfListRequestPBImpl(YarnServerRouterProtos.GetPelotonZKConfListRequestProto proto) {
    setProto(proto);
  }
  @Override
  public YarnServerRouterProtos.GetPelotonZKConfListRequestProto getProto() {
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
