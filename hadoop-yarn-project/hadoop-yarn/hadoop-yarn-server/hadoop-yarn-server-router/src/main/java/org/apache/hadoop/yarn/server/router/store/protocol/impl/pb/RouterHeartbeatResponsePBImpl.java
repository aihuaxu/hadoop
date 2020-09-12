package org.apache.hadoop.yarn.server.router.store.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.store.record.PBRecord;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RouterHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.router.protocol.RouterProtocolPBTranslator;
import org.apache.hadoop.yarn.server.router.store.protocol.RouterHeartbeatResponse;
import org.apache.hadoop.yarn.proto.YarnServerRouterProtos.RouterHeartbeatResponseProto;

import java.io.IOException;

/**
 * Protobuf implementation of the state store API object
 * RouterHeartbeatResponse.
 */
public class RouterHeartbeatResponsePBImpl extends RouterHeartbeatResponse
    implements PBRecord {

  private RouterProtocolPBTranslator<RouterHeartbeatResponseProto,
        RouterHeartbeatResponseProto.Builder,
        RouterHeartbeatResponseProtoOrBuilder> translator =
      new RouterProtocolPBTranslator<RouterHeartbeatResponseProto,
          RouterHeartbeatResponseProto.Builder,
          RouterHeartbeatResponseProtoOrBuilder>(
          RouterHeartbeatResponseProto.class);

  public RouterHeartbeatResponsePBImpl() {
  }

  @Override
  public RouterHeartbeatResponseProto getProto() {
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
  public boolean getStatus() {
    return this.translator.getProtoOrBuilder().getStatus();
  }

  @Override
  public void setStatus(boolean result) {
    this.translator.getBuilder().setStatus(result);
  }
}

