package org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PelotonZKConfPBImpl extends PelotonZKConf implements PBRecord {
  private static final Logger LOG =
      LoggerFactory.getLogger(PelotonZKConfPBImpl.class);

  private RouterProtocolPBTranslator<YarnServerRouterProtos.PelotonZKConfProto, YarnServerRouterProtos.PelotonZKConfProto.Builder,
      YarnServerRouterProtos.PelotonZKConfProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.PelotonZKConfProto, YarnServerRouterProtos.PelotonZKConfProto.Builder,
                YarnServerRouterProtos.PelotonZKConfProtoOrBuilder>(YarnServerRouterProtos.PelotonZKConfProto.class);

  public PelotonZKConfPBImpl() {}

  public PelotonZKConfPBImpl(YarnServerRouterProtos.PelotonZKConfProto proto) {
    this.translator.setProto(proto);
  }


  @Override
  public String getCluster() {
    YarnServerRouterProtos.PelotonZKConfProtoOrBuilder proto = translator.getProtoOrBuilder();
    if (!proto.hasCluster()) {
      return null;
    }
    return proto.getCluster();
  }

  @Override
  public void setCluster(String cluster) {
    YarnServerRouterProtos.PelotonZKConfProto.Builder builder = translator.getBuilder();
    if (cluster == null || cluster.isEmpty()) {
      builder.clearCluster();
    } else {
      builder.setCluster(cluster);
    }
  }

  @Override
  public List<PelotonZKInfo> getPelotonZKInfoList() {
    YarnServerRouterProtos.PelotonZKConfProtoOrBuilder proto = translator.getProtoOrBuilder();
    List<PelotonZKInfo> zkInfoList = new ArrayList<>();
    for(YarnServerRouterProtos.PelotonZKInfoProto zkInfoProto: proto.getZkInfosList()) {
      zkInfoList.add(new PelotonZKInfoPBImpl(zkInfoProto));
    }
    return zkInfoList;
  }

  @Override
  public void setPelotonZKInfo(List<PelotonZKInfo> zkInfoList) {
    YarnServerRouterProtos.PelotonZKConfProto.Builder builder = translator.getBuilder();
    builder.clearZkInfos();
    List<YarnServerRouterProtos.PelotonZKInfoProto> zkInfoListProto = new ArrayList<>();
    for(PelotonZKInfo zkInfo: zkInfoList) {
      if (zkInfo instanceof PelotonZKInfoPBImpl) {
        PelotonZKInfoPBImpl zkInfoPB = (PelotonZKInfoPBImpl) zkInfo;
        zkInfoListProto.add(zkInfoPB.getProto());
      }
    }
    builder.addAllZkInfos(zkInfoListProto);
  }

  @Override
  public void setDateModified(long time) {
    this.translator.getBuilder().setDateModified(time);
  }

  @Override
  public long getDateModified() {
    return this.translator.getBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    this.translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return this.translator.getBuilder().getDateCreated();
  }

  @Override
  public long getExpirationMs() {
    return 0;
  }

  @Override
  public YarnServerRouterProtos.PelotonZKConfProto getProto() {
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
