/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.SetPoolIpcConnectionsRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.SetPoolIpcConnectionsRequestProto.Builder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.SetPoolIpcConnectionsRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.SetPoolIpcConnectionsRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import java.io.IOException;

/**
 * Protobuf implementation of the state store API object
 * GetSafeModeRequest.
 */
public class SetPoolIpcConnectionsRequestPBImpl extends SetPoolIpcConnectionsRequest
    implements PBRecord {

  private FederationProtocolPBTranslator<SetPoolIpcConnectionsRequestProto,
      Builder, SetPoolIpcConnectionsRequestProtoOrBuilder> translator =
          new FederationProtocolPBTranslator<>(SetPoolIpcConnectionsRequestProto.class);

  public SetPoolIpcConnectionsRequestPBImpl() {
  }

  public SetPoolIpcConnectionsRequestPBImpl(SetPoolIpcConnectionsRequestProto proto) {
    this.translator.setProto(proto);
  }

  @Override
  public SetPoolIpcConnectionsRequestProto getProto() {
    return translator.build();
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
  public String getNsId() {
    return this.translator.getProtoOrBuilder().getNsId();
  }

  @Override
  public void setNsId(String nsId) {
    this.translator.getBuilder().setNsId(nsId);
  }

  @Override
  public String getUser() {
    return this.translator.getProtoOrBuilder().getUser();
  }

  @Override
  public void setUser(String user) {
    this.translator.getBuilder().setUser(user);
  }

  @Override
  public int getNumConnections() {
    return this.translator.getProtoOrBuilder().getNumConnections();
  }

  @Override
  public void setNumConnections(int numConnections) {
    this.translator.getBuilder().setNumConnections(numConnections);
  }
}
