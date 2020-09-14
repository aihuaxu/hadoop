package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.ClearAllPelotonZKConfsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.ClearAllPelotonZKConfsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class ClearAllPelotonZKConfsRequestPBImpl extends ClearAllPelotonZKConfsRequest implements PBRecord {

  private RouterProtocolPBTranslator<ClearAllPelotonZKConfsRequestProto,
        ClearAllPelotonZKConfsRequestProto.Builder,
        ClearAllPelotonZKConfsRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<ClearAllPelotonZKConfsRequestProto,
          ClearAllPelotonZKConfsRequestProto.Builder,
          ClearAllPelotonZKConfsRequestProtoOrBuilder>(
          ClearAllPelotonZKConfsRequestProto.class);

  public ClearAllPelotonZKConfsRequestPBImpl() {}
  public ClearAllPelotonZKConfsRequestPBImpl(ClearAllPelotonZKConfsRequestProto proto) {
    setProto(proto);
  }
  @Override
  public ClearAllPelotonZKConfsRequestProto getProto() {
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