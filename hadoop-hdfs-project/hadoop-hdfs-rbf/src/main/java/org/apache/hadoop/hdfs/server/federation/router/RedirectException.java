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
package org.apache.hadoop.hdfs.server.federation.router;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;

/**
 * Thrown by a remote server when it is up, but is not the active server in a
 * set of servers in which only a subset may be active.
 */
@InterfaceStability.Evolving
public class RedirectException extends IOException {
  static final long serialVersionUID = 0x12308AD010L;
  private List<RemoteLocation> locations;

  public RedirectException() {}
  public RedirectException(String msg) {
    super(msg);
  }

  public List<RemoteLocation> getLocations() {
    return locations;
  }

  public void setLocations(List<RemoteLocation> locations) {
    this.locations = locations;
  }
}