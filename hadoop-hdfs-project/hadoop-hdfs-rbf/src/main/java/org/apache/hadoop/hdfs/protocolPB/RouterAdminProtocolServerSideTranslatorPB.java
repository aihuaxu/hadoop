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
package org.apache.hadoop.hdfs.protocolPB;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.AddMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.EnterSafeModeRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.EnterSafeModeResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetMountTableEntriesRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetMountTableEntriesResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetSafeModeRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetSafeModeResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRemoteLocationRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRemoteLocationResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.LeaveSafeModeRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.LeaveSafeModeResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoveMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.UpdateMountTableEntryResponseProto;
import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.EnterSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRemoteLocationRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRemoteLocationResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.LeaveSafeModeResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.AddMountTableEntryRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.AddMountTableEntryResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.EnterSafeModeRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.EnterSafeModeResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.GetMountTableEntriesRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.GetMountTableEntriesResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.GetRemoteLocationRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.GetRemoteLocationResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.GetSafeModeRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.GetSafeModeResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.LeaveSafeModeRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.LeaveSafeModeResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.RemoveMountTableEntryRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.RemoveMountTableEntryResponsePBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.UpdateMountTableEntryRequestPBImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb.UpdateMountTableEntryResponsePBImpl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is used on the server side. Calls come across the wire for the for
 * protocol {@link RouterAdminProtocolPB}. This class translates the PB data
 * types to the native data types used inside the HDFS Router as specified in
 * the generic RouterAdminProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class RouterAdminProtocolServerSideTranslatorPB implements
    RouterAdminProtocolPB {

  private final RouterAdminServer server;

  /**
   * Constructor.
   * @param server The NN server.
   * @throws IOException
   */
  public RouterAdminProtocolServerSideTranslatorPB(RouterAdminServer server)
      throws IOException {
    this.server = server;
  }

  @Override
  public AddMountTableEntryResponseProto addMountTableEntry(
      RpcController controller, AddMountTableEntryRequestProto request)
      throws ServiceException {

    try {
      AddMountTableEntryRequest req =
          new AddMountTableEntryRequestPBImpl(request);
      AddMountTableEntryResponse response = server.addMountTableEntry(req);
      AddMountTableEntryResponsePBImpl responsePB =
          (AddMountTableEntryResponsePBImpl)response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Remove an entry from the mount table.
   */
  @Override
  public RemoveMountTableEntryResponseProto removeMountTableEntry(
      RpcController controller, RemoveMountTableEntryRequestProto request)
      throws ServiceException {
    try {
      RemoveMountTableEntryRequest req =
          new RemoveMountTableEntryRequestPBImpl(request);
      RemoveMountTableEntryResponse response =
          server.removeMountTableEntry(req);
      RemoveMountTableEntryResponsePBImpl responsePB =
          (RemoveMountTableEntryResponsePBImpl)response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get matching mount table entries.
   */
  @Override
  public GetMountTableEntriesResponseProto getMountTableEntries(
      RpcController controller, GetMountTableEntriesRequestProto request)
          throws ServiceException {
    try {
      GetMountTableEntriesRequest req =
          new GetMountTableEntriesRequestPBImpl(request);
      GetMountTableEntriesResponse response = server.getMountTableEntries(req);
      GetMountTableEntriesResponsePBImpl responsePB =
          (GetMountTableEntriesResponsePBImpl)response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Update a single mount table entry.
   */
  @Override
  public UpdateMountTableEntryResponseProto updateMountTableEntry(
      RpcController controller, UpdateMountTableEntryRequestProto request)
          throws ServiceException {
    try {
      UpdateMountTableEntryRequest req =
          new UpdateMountTableEntryRequestPBImpl(request);
      UpdateMountTableEntryResponse response =
          server.updateMountTableEntry(req);
      UpdateMountTableEntryResponsePBImpl responsePB =
          (UpdateMountTableEntryResponsePBImpl)response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public EnterSafeModeResponseProto enterSafeMode(RpcController controller,
      EnterSafeModeRequestProto request) throws ServiceException {
    try {
      EnterSafeModeRequest req = new EnterSafeModeRequestPBImpl(request);
      EnterSafeModeResponse response = server.enterSafeMode(req);
      EnterSafeModeResponsePBImpl responsePB =
          (EnterSafeModeResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public LeaveSafeModeResponseProto leaveSafeMode(RpcController controller,
      LeaveSafeModeRequestProto request) throws ServiceException {
    try {
      LeaveSafeModeRequest req = new LeaveSafeModeRequestPBImpl(request);
      LeaveSafeModeResponse response = server.leaveSafeMode(req);
      LeaveSafeModeResponsePBImpl responsePB =
          (LeaveSafeModeResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetSafeModeResponseProto getSafeMode(RpcController controller,
      GetSafeModeRequestProto request) throws ServiceException {
    try {
      GetSafeModeRequest req = new GetSafeModeRequestPBImpl(request);
      GetSafeModeResponse response = server.getSafeMode(req);
      GetSafeModeResponsePBImpl responsePB =
          (GetSafeModeResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Get destination for given source path.
   */
  @Override
  public GetRemoteLocationResponseProto getRemoteLocation(RpcController controller, GetRemoteLocationRequestProto request)
      throws ServiceException {
    try {
      GetRemoteLocationRequest req =
          new GetRemoteLocationRequestPBImpl(request);
      GetRemoteLocationResponse response = server.getRemoteLocation(req);
      GetRemoteLocationResponsePBImpl responsePB =
          (GetRemoteLocationResponsePBImpl) response;
      return responsePB.getProto();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
