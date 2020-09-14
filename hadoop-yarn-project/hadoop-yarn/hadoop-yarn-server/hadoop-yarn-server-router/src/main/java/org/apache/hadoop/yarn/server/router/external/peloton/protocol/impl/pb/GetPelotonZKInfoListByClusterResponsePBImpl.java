package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb.PelotonZKInfoPBImpl;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetPelotonZKInfoListByClusterResponsePBImpl extends GetPelotonZKInfoListByClusterResponse implements PBRecord {

  private RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto,
        YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto.Builder,
        YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto,
          YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto.Builder,
          YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProtoOrBuilder>(
          YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto.class);

  public GetPelotonZKInfoListByClusterResponsePBImpl() {}

  public GetPelotonZKInfoListByClusterResponsePBImpl(YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto proto) {
    setProto(proto);
  }
  @Override
  public List<PelotonZKInfo> getPelotonZKInfoList() {
    List<YarnServerRouterProtos.PelotonZKInfoProto> zkInfoProtos = translator.getProtoOrBuilder().getZkInfosList();
    List<PelotonZKInfo> result = new ArrayList<>();
    for(YarnServerRouterProtos.PelotonZKInfoProto zkInfoProto: zkInfoProtos) {
      result.add(new PelotonZKInfoPBImpl(zkInfoProto));
    }
    return result;
  }

  @Override
  public void setPelotonZKInfoList(List<PelotonZKInfo> zkInfoList) {
    translator.getBuilder().clearZkInfos();
    for(PelotonZKInfo zkInfo: zkInfoList) {
      if (zkInfo instanceof PelotonZKInfoPBImpl) {
        PelotonZKInfoPBImpl zkInfoPB = (PelotonZKInfoPBImpl) zkInfo;
        translator.getBuilder().addZkInfos(zkInfoPB.getProto());
      }
    }
  }

  @Override
  public YarnServerRouterProtos.GetPelotonZKInfoListByClusterResponseProto getProto() {
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

