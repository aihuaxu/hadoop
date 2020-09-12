package org.apache.hadoop.yarn.server.router.store.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RouterRecordProto;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RouterHeartbeatRequestProto;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatRequest;
import org.apache.hadoop.yarn.server.router.store.records.RouterState;
import org.apache.hadoop.yarn.server.router.store.records.impl.pb.RouterStatePBImpl;

import java.io.IOException;

public class RouterHeartbeatRequestPBImpl extends RouterHeartbeatRequest implements PBRecord {

  private RouterProtocolPBTranslator<RouterHeartbeatRequestProto, RouterHeartbeatRequestProto.Builder,
        YarnServerRouterProtos.RouterHeartbeatRequestProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<RouterHeartbeatRequestProto,
          RouterHeartbeatRequestProto.Builder, YarnServerRouterProtos.RouterHeartbeatRequestProtoOrBuilder>(
          RouterHeartbeatRequestProto.class);

  public RouterHeartbeatRequestPBImpl() {
  }

  @Override
  public RouterHeartbeatRequestProto getProto() {
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

  @Override
  public RouterState getRouter() throws IOException {
    RouterRecordProto routerProto =
        this.translator.getProtoOrBuilder().getRouter();
    return new RouterStatePBImpl(routerProto);
  }

  @Override
  public void setRouter(RouterState routerState) {
    if (routerState instanceof RouterStatePBImpl) {
      RouterStatePBImpl routerStatePB = (RouterStatePBImpl)routerState;
      this.translator.getBuilder().setRouter(routerStatePB.getProto());
    }
  }
}

