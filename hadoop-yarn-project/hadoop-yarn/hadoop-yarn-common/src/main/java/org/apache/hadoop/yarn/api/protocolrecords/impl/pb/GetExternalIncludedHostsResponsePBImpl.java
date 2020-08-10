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
import org.apache.hadoop.yarn.api.protocolrecords.GetExternalIncludedHostsResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetExternalIncludedHostsResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetExternalIncludedHostsResponseProtoOrBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GetExternalIncludedHostsResponsePBImpl extends GetExternalIncludedHostsResponse {

  GetExternalIncludedHostsResponseProto proto =
          GetExternalIncludedHostsResponseProto.getDefaultInstance();
  GetExternalIncludedHostsResponseProto.Builder builder = null;
  private Set<String> includedHosts;
  boolean viaProto = false;

  public GetExternalIncludedHostsResponsePBImpl() {
    builder = GetExternalIncludedHostsResponseProto.newBuilder();
  }

  public GetExternalIncludedHostsResponsePBImpl(GetExternalIncludedHostsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized GetExternalIncludedHostsResponseProto getProto() {
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
    if (this.includedHosts != null) {
      addIncludedHostsToProto();
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetExternalIncludedHostsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void addIncludedHostsToProto() {
    maybeInitBuilder();
    builder.clearIncludedHosts();
    builder.addAllIncludedHosts(this.includedHosts);
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

  private void initIncludedHosts() {
    GetExternalIncludedHostsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<String> includedHosts = p.getIncludedHostsList();
    this.includedHosts = new HashSet<>();
    this.includedHosts.addAll(includedHosts);
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public synchronized void setIncludedHosts(Set<String> includedHosts) {
    maybeInitBuilder();
    this.includedHosts = new HashSet<>();
    if (includedHosts == null) {
      builder.clearIncludedHosts();
      return;
    }
    this.includedHosts.addAll(includedHosts);
  }

  @Override
  public synchronized Set<String> getIncludedHosts() {
    if (this.includedHosts != null) {
      return this.includedHosts;
    }
    initIncludedHosts();
    return this.includedHosts;
  }
}
