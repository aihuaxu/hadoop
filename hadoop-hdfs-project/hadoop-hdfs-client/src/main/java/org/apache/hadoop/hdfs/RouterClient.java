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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientRouterProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientRouterProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientRouterProtocolTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Client to connect to the router server via the client router protocol.
 */
public class RouterClient {

    private final ClientRouterProtocol proxy;
    private final UserGroupInformation ugi;

    public RouterClient(InetSocketAddress address, Configuration conf)
            throws IOException {
        this.ugi = UserGroupInformation.getCurrentUser();
        this.proxy = createRouterProxy(address, conf, ugi);
    }

    private static ClientRouterProtocol createRouterProxy(
            InetSocketAddress address, Configuration conf, UserGroupInformation ugi)
            throws IOException {

        RPC.setProtocolEngine(
                conf, ClientRouterProtocolPB.class, ProtobufRpcEngine.class);

        AtomicBoolean fallbackToSimpleAuth = new AtomicBoolean(false);
        final long version = RPC.getProtocolVersion(ClientRouterProtocolPB.class);
        ClientRouterProtocolPB proxy = RPC.getProtocolProxy(
                ClientRouterProtocolPB.class, version, address, ugi, conf,
                NetUtils.getDefaultSocketFactory(conf),
                RPC.getRpcTimeout(conf), null,
                fallbackToSimpleAuth).getProxy();

        return new ClientRouterProtocolTranslatorPB(proxy);
    }

    /**
     * getRemoteLocation wraps implementation from ClientRouterProtocol.
     *
     * @param src the router path to be resolved
     * @return a resolved path
     * @throws IOException exception during remove RPC call
     */
    public Path getRemoteLocation(String src) throws IOException{
        return proxy.getRemoteLocation(src);
    }
}
