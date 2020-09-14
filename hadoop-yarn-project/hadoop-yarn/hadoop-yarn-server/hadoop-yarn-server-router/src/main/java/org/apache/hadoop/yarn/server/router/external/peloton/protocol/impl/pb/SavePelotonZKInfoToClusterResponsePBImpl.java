package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class SavePelotonZKInfoToClusterResponsePBImpl extends SavePelotonZKInfoToClusterResponse implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto,
      YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto.Builder,
      YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto,
                YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto.Builder,
                YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProtoOrBuilder>(
          YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto.class);

  public SavePelotonZKInfoToClusterResponsePBImpl() {}

  public SavePelotonZKInfoToClusterResponsePBImpl(YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto proto) {
    translator.setProto(proto);
  }

  @Override
  public boolean getStatus() {
    return translator.getProtoOrBuilder().getStatus();
  }

  @Override
  public void setStatus(boolean result) {
    translator.getBuilder().setStatus(result);
  }

  @Override
  public void setErrorMessage(String errorMessage) {
    translator.getBuilder().setErrorMsg(errorMessage);
  }

  @Override
  public String getErrorMessage() {
    return translator.getProtoOrBuilder().getErrorMsg();
  }

  @Override
  public YarnServerRouterProtos.SavePelotonZKInfoToClusterResponseProto getProto() {
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
