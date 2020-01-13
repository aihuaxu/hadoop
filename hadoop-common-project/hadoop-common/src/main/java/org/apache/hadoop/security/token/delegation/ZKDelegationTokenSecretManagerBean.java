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
package org.apache.hadoop.security.token.delegation;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * JMX interface for the zookeeper based delegation token secret manager
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ZKDelegationTokenSecretManagerBean {
  /**
   * Returns how many times a cache miss happened to renew tokens
   * @return
   */
  long getRenewTokenCacheMiss();

  /**
   * Returns how many times a cache hit happened to renew tokens
   * @return
   */
  long getRenewTokenCacheHit();

  /**
   * Returns how many times a cache miss happened to cancel tokens
   * @return
   */
  long getCancelTokenCacheMiss();

  /**
   * Returns how many times a cache hit happened to cancel tokens
   * @return
   */
  long getCancelTokenCacheHit();

  /**
   * Returns how many times a cache miss happened to read tokens
   * @return
   */
  long getReadTokenCacheMiss();

  /**
   * Returns how many times a cache hit happened to read tokens
   * @return
   */
  long getReadTokenCacheHit();

}
