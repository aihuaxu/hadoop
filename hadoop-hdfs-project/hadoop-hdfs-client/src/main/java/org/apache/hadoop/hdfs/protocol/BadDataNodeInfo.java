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

/**
 * Used by admin to report potential bad DataNodes
 */
public class BadDataNodeInfo {
  private final String ipAddr;
  private final int xferPort;
  private final boolean markedBad;

  public BadDataNodeInfo(String ipAddr, int xferPort, boolean markedBad) {
    this.ipAddr = ipAddr;
    this.xferPort = xferPort;
    this.markedBad = markedBad;
  }

  public String getIpAddr() {
    return ipAddr;
  }

  public int getXferPort() {
    return xferPort;
  }

  public boolean isMarkedBad() {
    return markedBad;
  }

  @Override
  public String toString() {
    return ipAddr + ":" + xferPort + ":" +
            (markedBad ? "bad" : "normal");
  }
}
