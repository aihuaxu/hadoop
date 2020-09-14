package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class ClearAllPelotonZKConfsResponsePBImpl extends ClearAllPelotonZKConfsResponse implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto,
        YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto.Builder,
        YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto,
          YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto.Builder,
          YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProtoOrBuilder>(
          YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto.class);

  public ClearAllPelotonZKConfsResponsePBImpl() {}

  public ClearAllPelotonZKConfsResponsePBImpl(YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto proto) {
    setProto(proto);
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
  public YarnServerRouterProtos.ClearAllPelotonZKConfsResponseProto getProto() {
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

