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
package org.apache.hadoop.hdfs.server.federation.store.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;

import java.io.IOException;
import java.util.List;

/**
 * API request for verifying if current Router state is safe mode.
 */
public abstract class SetPoolIpcConnectionsRequest {
  public static SetPoolIpcConnectionsRequest newInstance() throws IOException {
    return StateStoreSerializer.newRecord(SetPoolIpcConnectionsRequest.class);
  }

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract String getNsId();

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setNsId(String nsId);

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract String getUser();

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setUser(String user);

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract int getNumConnections();

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public abstract void setNumConnections(int numConnections);
}