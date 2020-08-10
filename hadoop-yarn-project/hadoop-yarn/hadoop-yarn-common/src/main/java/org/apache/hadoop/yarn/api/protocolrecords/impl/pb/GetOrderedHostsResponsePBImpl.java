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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetOrderedHostsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetOrderedHostsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetOrderedHostsResponseProtoOrBuilder;
import java.util.ArrayList;
import java.util.List;

public class GetOrderedHostsResponsePBImpl extends GetOrderedHostsResponse {

  GetOrderedHostsResponseProto proto =
    GetOrderedHostsResponseProto.getDefaultInstance();
  GetOrderedHostsResponseProto.Builder builder = null;
  private List<String> orderedHosts;
  boolean viaProto = false;

  public GetOrderedHostsResponsePBImpl() {
    builder = GetOrderedHostsResponseProto.newBuilder();
  }

  public GetOrderedHostsResponsePBImpl(GetOrderedHostsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized GetOrderedHostsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.orderedHosts != null) {
      getOrderedHostsToProto();
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetOrderedHostsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void getOrderedHostsToProto() {
    maybeInitBuilder();
    builder.clearOrderedHosts();
    builder.addAllOrderedHosts(this.orderedHosts);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  private void initOrderedHosts() {
    GetOrderedHostsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<String> orderedHosts = p.getOrderedHostsList();
    this.orderedHosts = new ArrayList<>();
    this.orderedHosts.addAll(orderedHosts);
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public synchronized void setOrderedHosts(List<String> orderedHosts) {
    maybeInitBuilder();
    this.orderedHosts = new ArrayList<>();
    if (orderedHosts == null) {
      builder.clearOrderedHosts();
      return;
    }
    this.orderedHosts.addAll(orderedHosts);
  }

  @Override
  public synchronized List<String> getOrderedHosts() {
    if (this.orderedHosts != null) {
      return this.orderedHosts;
    }
    initOrderedHosts();
    return this.orderedHosts;
  }
}
