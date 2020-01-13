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

import java.io.IOException;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.metrics2.util.MBeans;

/**
 * ZKDelegationTokenSecretManagerMetrics manages the zk metrics
 */
public class ZKDelegationTokenSecretManagerMetrics
    implements ZKDelegationTokenSecretManagerBean {

  /** JMX bean. */
  private ObjectName beanName;

  private long renewHit;
  private long renewMiss;
  private long cancelHit;
  private long cancelMiss;
  private long readHit;
  private long readMiss;

  public ZKDelegationTokenSecretManagerMetrics() {
    try {
      StandardMBean bean = new StandardMBean(this,
          ZKDelegationTokenSecretManagerBean.class);
      this.beanName = MBeans.register("Zookeeper", "DelegationTokenOp", bean);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad ZK MBean setup", e);
    }
  }

  /**
   * Unregister the JMX beans.
   */
  public void close() {
    if (this.beanName != null) {
      MBeans.unregister(beanName);
    }
  }

  @Override
  public long getRenewTokenCacheMiss() {
    return renewMiss;
  }

  @Override
  public long getRenewTokenCacheHit() {
    return renewHit;
  }

  @Override
  public long getCancelTokenCacheMiss() {
    return cancelMiss;
  }

  @Override
  public long getCancelTokenCacheHit() {
    return cancelHit;
  }

  @Override
  public long getReadTokenCacheMiss() {
    return readMiss;
  }

  @Override
  public long getReadTokenCacheHit() {
    return readHit;
  }

  public void addHit(AbstractDelegationTokenSecretManager.TokenOp op) {
    switch(op) {
      case RENEW:
        this.renewHit++;
        break;
      case CANCEL:
        this.cancelHit++;
        break;
      default:
        this.readHit++;
    }
  }

  public void addMiss(AbstractDelegationTokenSecretManager.TokenOp op) {
    switch(op) {
      case RENEW:
        this.renewMiss++;
        break;
      case CANCEL:
        this.cancelMiss++;
        break;
      default:
        this.readMiss++;
    }
  }
}
