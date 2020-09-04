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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientRouterProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientRouterProtocolProtos.GetRemoteLocationRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientRouterProtocolProtos.GetRemoteLocationResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientRouterProtocolProtos.RemoteLocationProto;
import org.apache.hadoop.ipc.*;

import com.google.protobuf.ServiceException;

/**
 * This class forwards all router calls to router servers.
 */
public class ClientRouterProtocolTranslatorPB implements ProtocolMetaInterface,
        Closeable, ProtocolTranslator, ClientRouterProtocol {
    final private ClientRouterProtocolPB rpcProxy;

    public ClientRouterProtocolTranslatorPB(ClientRouterProtocolPB proxy) {
        rpcProxy = proxy;
    }

    @Override
    public void close() throws IOException {
        RPC.stopProxy(rpcProxy);
    }

    @Override
    public boolean isMethodSupported(String methodName) throws IOException {
        return RpcClientUtil.isMethodSupported(rpcProxy,
                ClientNamenodeProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                RPC.getProtocolVersion(ClientNamenodeProtocolPB.class), methodName);
    }

    @Override
    public Object getUnderlyingProxyObject() {
        return rpcProxy;
    }

    /**
     * getRemoteLocation sends request in proto format and extract parsed path from proto response.
     *
     * @param src the router path to be resolved
     * @return the resolved path
     * @throws IOException exception happens during the rpc call
     */
    @Override
    public Path getRemoteLocation(String src) throws IOException {
        GetRemoteLocationRequestProto req = GetRemoteLocationRequestProto
                .newBuilder()
                .setSrcPath(src)
                .build();
        try {
            GetRemoteLocationResponseProto resp = rpcProxy.getRemoteLocation(null, req);
            int count = resp.getRemoteLocationCount();
            if (count == 0) throw new RuntimeException("No result path found");
            RemoteLocationProto remoteLocationProto = resp.getRemoteLocation(0);
            return PBHelperClient.convertRemoteLocationProto(resp.getRemoteLocationList()).get(0);
        } catch (ServiceException e) {
            throw ProtobufHelper.getRemoteException(e);
        }
    }
}
