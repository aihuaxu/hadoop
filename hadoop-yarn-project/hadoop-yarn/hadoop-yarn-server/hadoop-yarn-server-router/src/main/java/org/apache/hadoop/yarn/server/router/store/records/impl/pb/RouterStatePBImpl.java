package org.apache.hadoop.yarn.server.router.store.records.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RouterRecordProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RouterRecordProto;
import org.apache.hadoop.yarn.server.router.RouterServiceState;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;

import java.io.IOException;

public class RouterStatePBImpl extends RouterState implements PBRecord {
  private RouterProtocolPBTranslator<RouterRecordProto, RouterRecordProto.Builder,
      RouterRecordProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<RouterRecordProto, RouterRecordProto.Builder,
          RouterRecordProtoOrBuilder>(RouterRecordProto.class);

  public RouterStatePBImpl() {
  }

  public RouterStatePBImpl(RouterRecordProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void setAddress(String address) {
    RouterRecordProto.Builder builder = this.translator.getBuilder();
    if (address == null) {
      builder.clearAddress();
    } else {
      builder.setAddress(address);
    }
  }

  @Override
  public void setDateStarted(long dateStarted) {
    this.translator.getBuilder().setDateStarted(dateStarted);
  }

  @Override
  public String getAddress() {
    RouterRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (!proto.hasAddress()) {
      return null;
    }
    return proto.getAddress();
  }

  @Override
  public RouterServiceState getStatus() {
    RouterRecordProtoOrBuilder proto = this.translator.getProtoOrBuilder();
    if (!proto.hasStatus()) {
      return null;
    }
    return RouterServiceState.valueOf(proto.getStatus());
  }

  @Override
  public void setStatus(RouterServiceState newStatus) {
    RouterRecordProto.Builder builder = this.translator.getBuilder();
    if (newStatus == null) {
      builder.clearStatus();
    } else {
      builder.setStatus(newStatus.toString());
    }
  }

  @Override
  public long getDateStarted() {
    return this.translator.getProtoOrBuilder().getDateStarted();
  }

  @Override
  public void setDateModified(long time) {
    this.translator.getBuilder().setDateModified(time);
  }

  @Override
  public long getDateModified() {
    return this.translator.getProtoOrBuilder().getDateModified();
  }

  @Override
  public void setDateCreated(long time) {
    this.translator.getBuilder().setDateCreated(time);
  }

  @Override
  public long getDateCreated() {
    return this.translator.getProtoOrBuilder().getDateCreated();
  }

  @Override
  public RouterRecordProto getProto() {
    return this.translator.build();
  }

  @Override
  public void setProto(Message proto) {
    this.translator.setProto(proto);
  }

  @Override
  public void readInstance(String base64String) throws IOException {
    this.translator.readInstance(base64String);
  }
}
