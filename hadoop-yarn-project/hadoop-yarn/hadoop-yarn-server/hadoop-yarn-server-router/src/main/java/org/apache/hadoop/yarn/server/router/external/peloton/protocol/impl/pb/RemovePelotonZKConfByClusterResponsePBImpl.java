package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class RemovePelotonZKConfByClusterResponsePBImpl extends RemovePelotonZKConfByClusterResponse implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto,
      YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto.Builder,
      YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto,
                YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto.Builder,
                YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProtoOrBuilder>(
          YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto.class);

  public RemovePelotonZKConfByClusterResponsePBImpl() {}

  public RemovePelotonZKConfByClusterResponsePBImpl(YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto proto) {
    setProto(proto);
  }

  @Override
  public void setStatus(boolean result) {
    translator.getBuilder().setStatus(result);
  }

  @Override
  public boolean getStatus() {
    return translator.getProtoOrBuilder().getStatus();
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
  public YarnServerRouterProtos.RemovePelotonZKConfByClusterResponseProto getProto() {
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
