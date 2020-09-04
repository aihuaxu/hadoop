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
package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.TokenInfo;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

/**********************************************************************
 * ClientRouterProtocol is used by user code via the DistributedFileSystem class to
 * communicate with router.
 *
 * To be noticed that this protocol only exists in HDFS client side, the corresponding
 * rpc function in Router server side (hadoop-hdfs-rbf) is implemented in RouterAdminProtocol.
 * The reasons are below:
 *    1. make least changes and reuse as many codes as possible (no new protocol created at router side.)
 *    2. be able to backport client side change to hdfs 2.8 & 2.9
 *
 **********************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
        serverPrincipal = DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(DelegationTokenSelector.class)
public interface ClientRouterProtocol {

    long versionID = 1L;

    /**
     * Call router to parse given path.
     *
     * @param src source router path
     * @return A resolved path that is (supposed to be) the destination of src
     * @throws IOException
     */
    @Idempotent
    public Path getRemoteLocation(String src) throws IOException;
}
