package org.apache.hadoop.yarn.server.router.external.peloton.records.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;

import java.io.IOException;

public class PelotonZKInfoPBImpl extends PelotonZKInfo implements PBRecord {
  private RouterProtocolPBTranslator<YarnServerRouterProtos.PelotonZKInfoProto, YarnServerRouterProtos.PelotonZKInfoProto.Builder,
        YarnServerRouterProtos.PelotonZKInfoProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<YarnServerRouterProtos.PelotonZKInfoProto, YarnServerRouterProtos.PelotonZKInfoProto.Builder,
          YarnServerRouterProtos.PelotonZKInfoProtoOrBuilder>(YarnServerRouterProtos.PelotonZKInfoProto.class);

  public PelotonZKInfoPBImpl() {}

  public PelotonZKInfoPBImpl(YarnServerRouterProtos.PelotonZKInfoProto proto) {
    setProto(proto);
  }

  @Override
  public String getZone() {
    YarnServerRouterProtos.PelotonZKInfoProtoOrBuilder proto = translator.getProtoOrBuilder();
    if (!proto.hasZone()) {
      return null;
    }
    return proto.getZone();
  }

  @Override
  public void setZone(String zone) {
    YarnServerRouterProtos.PelotonZKInfoProto.Builder builder = translator.getBuilder();
    if (zone == null || zone.isEmpty()) {
      builder.clearZone();
    } else {
      builder.setZone(zone);
    }
  }

  @Override
  public String getRegion() {
    YarnServerRouterProtos.PelotonZKInfoProtoOrBuilder proto = translator.getProtoOrBuilder();
    if (!proto.hasRegion()) {
      return null;
    }
    return proto.getRegion();
  }

  @Override
  public void setRegion(String region) {
    YarnServerRouterProtos.PelotonZKInfoProto.Builder builder = translator.getBuilder();
    if (region == null || region.isEmpty()) {
      builder.clearRegion();
    } else {
      builder.setRegion(region);
    }
  }

  @Override
  public String getZKAddress() {
    YarnServerRouterProtos.PelotonZKInfoProtoOrBuilder proto = translator.getProtoOrBuilder();
    if (!proto.hasZkAddress()) {
      return null;
    }
    return proto.getZkAddress();
  }

  @Override
  public void setZKAddress(String zkAddress) {
    YarnServerRouterProtos.PelotonZKInfoProto.Builder builder = translator.getBuilder();
    if (zkAddress == null || zkAddress.isEmpty()) {
      builder.clearZkAddress();
    } else {
      builder.setZkAddress(zkAddress);
    }
  }

  @Override
  public String getResourcePoolPath() {
    YarnServerRouterProtos.PelotonZKInfoProtoOrBuilder proto = translator.getProtoOrBuilder();
    if (!proto.hasResourcePoolPath()) {
      return null;
    }
    return proto.getResourcePoolPath();
  }

  @Override
  public void setResourcePoolPath(String resourcePoolPath) {
    YarnServerRouterProtos.PelotonZKInfoProto.Builder builder = translator.getBuilder();
    if (resourcePoolPath == null || resourcePoolPath.isEmpty()) {
      builder.clearResourcePoolPath();
    } else {
      builder.setResourcePoolPath(resourcePoolPath);
    }
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

  @Override
  public YarnServerRouterProtos.PelotonZKInfoProto getProto() {
    return translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    translator.readInstance(base64String);
  }
}

