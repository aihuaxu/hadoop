package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb.PelotonZKConfPBImpl;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetPelotonZKConfListResponsePBImpl extends GetPelotonZKConfListResponse implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKConfListResponseProto,
      YarnServerRouterProtos.GetPelotonZKConfListResponseProto.Builder,
      YarnServerRouterProtos.GetPelotonZKConfListResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKConfListResponseProto,
          YarnServerRouterProtos.GetPelotonZKConfListResponseProto.Builder,
          YarnServerRouterProtos.GetPelotonZKConfListResponseProtoOrBuilder>(
          YarnServerRouterProtos.GetPelotonZKConfListResponseProto.class);

  public GetPelotonZKConfListResponsePBImpl() {}

  public GetPelotonZKConfListResponsePBImpl(YarnServerRouterProtos.GetPelotonZKConfListResponseProto proto) {
    setProto(proto);
  }
  @Override
  public List<PelotonZKConf> getPelotonZKConfList() {
    List<YarnServerRouterProtos.PelotonZKConfProto> zkConfs = translator.getProtoOrBuilder().getZkConfsList();
    List<PelotonZKConf> result = new ArrayList<>();
    for(YarnServerRouterProtos.PelotonZKConfProto zkConfProto: zkConfs) {
      result.add(new PelotonZKConfPBImpl(zkConfProto));
    }
    return result;
  }

  @Override
  public void setPelotonZKConfList(List<PelotonZKConf> zkConfs) {
    translator.getBuilder().clearZkConfs();
    for(PelotonZKConf zkConf: zkConfs) {
      if (zkConf instanceof PelotonZKConfPBImpl) {
        PelotonZKConfPBImpl zkConfPB = (PelotonZKConfPBImpl) zkConf;
        translator.getBuilder().addZkConfs(zkConfPB.getProto());
      }
    }
  }

  @Override
  public YarnServerRouterProtos.GetPelotonZKConfListResponseProto getProto() {
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

