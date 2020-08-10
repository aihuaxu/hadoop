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
import org.apache.hadoop.yarn.api.protocolrecords.GetExternalIncludedHostsRequest;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetExternalIncludedHostsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.GetExternalIncludedHostsRequestProtoOrBuilder;

public class GetExternalIncludedHostsRequestPBImpl extends GetExternalIncludedHostsRequest {

  GetExternalIncludedHostsRequestProto proto =
          GetExternalIncludedHostsRequestProto.getDefaultInstance();
  GetExternalIncludedHostsRequestProto.Builder builder = null;
  boolean viaProto = false;

  public GetExternalIncludedHostsRequestPBImpl() {
    builder = GetExternalIncludedHostsRequestProto.newBuilder();
  }

  public GetExternalIncludedHostsRequestPBImpl(GetExternalIncludedHostsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public GetExternalIncludedHostsRequestProto getProto() {
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
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetExternalIncludedHostsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  @Override
  public String getTag() {
    GetExternalIncludedHostsRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTag()) {
      return null;
    }
    return (p.getTag());
  }

  @Override
  public void setTag(String tag) {
    maybeInitBuilder();
    if (tag == null) {
      builder.clearTag();
      return;
    }
    builder.setTag(tag);
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
