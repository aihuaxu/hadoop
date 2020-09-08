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

package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.store.record.Query;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Common utility methods used by the Router server.
 *
 */
@Private
@Unstable
public final class RouterServerUtil {

  /** Disable constructor. */
  private RouterServerUtil() {
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(RouterServerUtil.class);

  /**
   * Throws an exception due to an error.
   *
   * @param errMsg the error message
   * @param t the throwable raised in the called class.
   * @throws YarnException on failure
   */
  @Public
  @Unstable
  public static void logAndThrowException(String errMsg, Throwable t)
      throws YarnException {
    if (t != null) {
      LOG.error(errMsg, t);
      throw new YarnException(errMsg, t);
    } else {
      LOG.error(errMsg);
      throw new YarnException(errMsg);
    }
  }

  /**
   * Log status of delegation token related operation.
   * Extend in future to use audit logger instead of local logging.
   */
  public static void logAuditEvent(boolean succeeded, String cmd, String tokenId)
          throws IOException {
    LOG.debug("Operation:" + cmd + " Status:" + succeeded + " TokenId:" + tokenId);
  }

  /**
   * Helper method to create instances of Object using the class name specified
   * in the configuration object.
   *
   * @param conf the yarn configuration
   * @param configuredClassName the configuration provider key
   * @param defaultValue the default implementation class
   * @param type the required interface/base class
   * @param <T> The type of the instance to create
   * @return the instances created
   */
  @SuppressWarnings("unchecked")
  public static <T> T createInstance(Configuration conf,
                                     String configuredClassName, String defaultValue, Class<T> type) {

    String className = conf.get(configuredClassName, defaultValue);
    try {
      Class<?> clusterResolverClass = conf.getClassByName(className);
      if (type.isAssignableFrom(clusterResolverClass)) {
        return (T) ReflectionUtils.newInstance(clusterResolverClass, conf);
      } else {
        throw new YarnRuntimeException("Class: " + className
                + " not instance of " + type.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate : " + className, e);
    }
  }

  /**
   * Get the base class for a record class. If we get an implementation of a
   * record we will return the real parent record class.
   *
   * @param clazz Class of the data record to check.
   * @return Base class for the record.
   */
  @SuppressWarnings("unchecked")
  public static <T extends BaseRecord>
  Class<? extends BaseRecord> getRecordClass(final Class<T> clazz) {

    // We ignore the Impl classes and go to the super class
    Class<? extends BaseRecord> actualClazz = clazz;
    while (actualClazz.getSimpleName().endsWith("Impl")) {
      actualClazz = (Class<? extends BaseRecord>) actualClazz.getSuperclass();
    }

    // Check if we went too far
    if (actualClazz.equals(BaseRecord.class)) {
      LOG.error("We went too far ({}) with {}", actualClazz, clazz);
      actualClazz = clazz;
    }
    return actualClazz;
  }

  /**
   * Get the base class name for a record. If we get an implementation of a
   * record we will return the real parent record class.
   *
   * @param clazz Class of the data record to check.
   * @return Name of the base class for the record.
   */
  public static <T extends BaseRecord> String getRecordName(
      final Class<T> clazz) {
    return getRecordClass(clazz).getSimpleName();
  }

  /**
   * Filters a list of records to find all records matching the query.
   *
   * @param query Map of field names and objects to use to filter results.
   * @param records List of data records to filter.
   * @return List of all records matching the query (or empty list if none
   *         match), null if the data set could not be filtered.
   */
  public static <T extends BaseRecord> List<T> filterMultiple(
      final Query<T> query, final Iterable<T> records) {

    List<T> matchingList = new ArrayList<>();
    for (T record : records) {
      if (query.matches(record)) {
        matchingList.add(record);
      }
    }
    return matchingList;
  }

}