package org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb.PelotonZKInfoPBImpl;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class SavePelotonZKInfoToClusterRequestPBImpl extends SavePelotonZKInfoToClusterRequest implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto,
      YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto.Builder,
      YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto,
                YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto.Builder,
                YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProtoOrBuilder>(
          YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto.class);

  public SavePelotonZKInfoToClusterRequestPBImpl() {}

  public SavePelotonZKInfoToClusterRequestPBImpl(YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto proto) {
    translator.setProto(proto);
  }

  @Override
  public void setCluster(String cluster) {
    translator.getBuilder().setCluster(cluster);
  }

  @Override
  public void setPelotonInfo(PelotonZKInfo zkInfo) {
    if (zkInfo != null) {
      PelotonZKInfoPBImpl pelotonZKInfoPB = (PelotonZKInfoPBImpl) zkInfo;
      YarnServerRouterProtos.PelotonZKInfoProto pelotonZKInfoProto = pelotonZKInfoPB.getProto();
      translator.getBuilder().setZkInfo(pelotonZKInfoProto);
    }
  }

  @Override
  public String getCluster() {
    return translator.getProtoOrBuilder().getCluster();
  }

  @Override
  public PelotonZKInfo getPelotonZkInfo() {
    YarnServerRouterProtos.PelotonZKInfoProto proto = translator.getProtoOrBuilder().getZkInfo();
    return new PelotonZKInfoPBImpl(proto);
  }

  @Override
  public void setIsCreateNew(boolean isCreate) {
    translator.getBuilder().setIsCreate(isCreate);
  }

  @Override
  public boolean getIsCreate() {
    return translator.getProtoOrBuilder().getIsCreate();
  }

  @Override
  public YarnServerRouterProtos.SavePelotonZKInfoToClusterRequestProto getProto() {
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

