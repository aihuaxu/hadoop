package org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonNodeLabel;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class PelotonNodeLabelPBImpl extends PelotonNodeLabel implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.PelotonNodeLabelProto, YarnServerRouterProtos.PelotonNodeLabelProto.Builder,
      YarnServerRouterProtos.PelotonNodeLabelProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.PelotonNodeLabelProto, YarnServerRouterProtos.PelotonNodeLabelProto.Builder,
          YarnServerRouterProtos.PelotonNodeLabelProtoOrBuilder>(YarnServerRouterProtos.PelotonNodeLabelProto.class);

  public PelotonNodeLabelPBImpl() {}

  public PelotonNodeLabelPBImpl(YarnServerRouterProtos.PelotonNodeLabelProto proto) {
    setProto(proto);
  }

  @Override
  public String getNodeLabel() {
    YarnServerRouterProtos.PelotonNodeLabelProtoOrBuilder proto = translator.getProtoOrBuilder();
    if (!proto.hasNodeLabel()) {
      return null;
    }
    return proto.getNodeLabel();
  }

  @Override
  public void setNodeLabel(String nodeLabel) {
    YarnServerRouterProtos.PelotonNodeLabelProto.Builder builder = translator.getBuilder();
    if (nodeLabel == null || nodeLabel.isEmpty()) {
      builder.clearNodeLabel();
    } else {
      builder.setNodeLabel(nodeLabel);
    }
  }

  @Override
  public YarnServerRouterProtos.PelotonNodeLabelProto getProto() {
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
  public void setDateModified(long time) {
    translator.getBuilder().setDateModified(time);
  }

  @Override
  public long getDateModified() {
    return translator.getBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return translator.getBuilder().getDateCreated();
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }
}
