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
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncludeExternalHostsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.IncludeExternalHostsRequestProtoOrBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IncludeExternalHostsRequestPBImpl extends IncludeExternalHostsRequest {

  Set<String> includedHosts = null;

  IncludeExternalHostsRequestProto proto =
          IncludeExternalHostsRequestProto.getDefaultInstance();
  IncludeExternalHostsRequestProto.Builder builder = null;
  boolean viaProto = false;

  public IncludeExternalHostsRequestPBImpl() {
    builder = IncludeExternalHostsRequestProto.newBuilder();
  }

  public IncludeExternalHostsRequestPBImpl(IncludeExternalHostsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public IncludeExternalHostsRequestProto getProto() {
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
    if (includedHosts != null && !includedHosts.isEmpty()) {
      builder.clearIncludedHosts();
      builder.addAllIncludedHosts(includedHosts);
    }
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = IncludeExternalHostsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void initIncludedHosts() {
    if (this.includedHosts != null) {
      return;
    }
    IncludeExternalHostsRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<String> includedHosts = p.getIncludedHostsList();
    this.includedHosts = new HashSet<>();
    this.includedHosts.addAll(includedHosts);
  }

  @Override
  public synchronized Set<String> getIncludedHosts() {
    initIncludedHosts();
    return this.includedHosts;
  }

  @Override
  public synchronized void setIncludedHosts(Set<String> includedHosts) {
    maybeInitBuilder();
    if (includedHosts == null)
      builder.clearIncludedHosts();
    this.includedHosts = includedHosts;
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
