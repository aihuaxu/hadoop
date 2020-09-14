package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb.PelotonZKConfPBImpl;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class SavePelotonZKConfRequestPBImpl extends SavePelotonZKConfRequest implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonZKConfRequestProto,
      YarnServerRouterProtos.SavePelotonZKConfRequestProto.Builder,
      YarnServerRouterProtos.SavePelotonZKConfRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonZKConfRequestProto,
                YarnServerRouterProtos.SavePelotonZKConfRequestProto.Builder,
                YarnServerRouterProtos.SavePelotonZKConfRequestProtoOrBuilder>(
          YarnServerRouterProtos.SavePelotonZKConfRequestProto.class);

  public SavePelotonZKConfRequestPBImpl() {}
  public SavePelotonZKConfRequestPBImpl(YarnServerRouterProtos.SavePelotonZKConfRequestProto proto) {
    setProto(proto);
  }
  @Override
  public void setPelotonZKConf(PelotonZKConf target) {
    if (target instanceof PelotonZKConfPBImpl) {
      PelotonZKConfPBImpl pelotonZKConfPB = (PelotonZKConfPBImpl) target;
      YarnServerRouterProtos.PelotonZKConfProto pelotonZKConfProto = pelotonZKConfPB.getProto();
      this.translator.getBuilder().setZkConf(pelotonZKConfProto);
    }
  }

  @Override
  public PelotonZKConf getPelotonZKConf() {
    YarnServerRouterProtos.PelotonZKConfProto pelotonZKConfProto = translator.getProtoOrBuilder().getZkConf();
    return new PelotonZKConfPBImpl(pelotonZKConfProto);
  }

  @Override
  public void setIsCreate(boolean isCreate) {
    translator.getBuilder().setIsCreate(isCreate);
  }

  @Override
  public boolean getIsCreate() {
    return translator.getProtoOrBuilder().getIsCreate();
  }

  @Override
  public YarnServerRouterProtos.SavePelotonZKConfRequestProto getProto() {
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

