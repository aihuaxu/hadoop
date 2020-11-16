package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class SavePelotonNodeLabelRequestPBImpl extends SavePelotonNodeLabelRequest implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonNodeLabelRequestProto,
      YarnServerRouterProtos.SavePelotonNodeLabelRequestProto.Builder,
      YarnServerRouterProtos.SavePelotonNodeLabelRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonNodeLabelRequestProto,
          YarnServerRouterProtos.SavePelotonNodeLabelRequestProto.Builder,
          YarnServerRouterProtos.SavePelotonNodeLabelRequestProtoOrBuilder>(
          YarnServerRouterProtos.SavePelotonNodeLabelRequestProto.class);

  public SavePelotonNodeLabelRequestPBImpl() {}

  public SavePelotonNodeLabelRequestPBImpl(YarnServerRouterProtos.SavePelotonNodeLabelRequestProto proto) {
    setProto(proto);
  }
  @Override
  public YarnServerRouterProtos.SavePelotonNodeLabelRequestProto getProto() {
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
  public void setNodeLabel(String label) {
    translator.getBuilder().setLabel(label);
  }

  @Override
  public String getNodeLabel() {
    return translator.getProtoOrBuilder().getLabel();
  }
}
