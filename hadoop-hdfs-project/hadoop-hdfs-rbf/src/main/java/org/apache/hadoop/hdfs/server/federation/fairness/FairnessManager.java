/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A fairness manager to hold policy controller that assign handlers
 * to name services. This helps prevent all handlers blocked on a single
 * name service.
 */
public class FairnessManager {

  private static final Logger LOG =
          LoggerFactory.getLogger(FairnessManager.class);

  private FairnessPolicyController fairnessPolicyController;

  public FairnessManager(Configuration conf) throws IOException {
    // Instantiate configured policy controller
    this.fairnessPolicyController = initPolicyHandler(conf);
  }

  /**
   * Creates an instance of a PolicyHandler from the configuration.
   *
   * @param conf Configuration that defines the policy handler class.
   * @return New policy handler.
   */
  public static FairnessPolicyController
  initPolicyHandler(Configuration conf) throws IOException {
    return FederationUtil.newFairnessPolicyResolver(conf);
  }

  public boolean grantPermission(String nsId)
          throws NoPermitAvailableException {
    try {
      fairnessPolicyController.acquirePermit(nsId);
    } catch (NoPermitAvailableException ex) {
      LOG.error("Unable to acquire permit for nsId:" + nsId);
      return false;
    }
    return true;
  }

  public void releasePermission(String nsId) {
    this.fairnessPolicyController.releasePermit(nsId);
  }

  public void shutdown() {
    if (fairnessPolicyController != null) {
      fairnessPolicyController.shutdown();
    }
  }
}
