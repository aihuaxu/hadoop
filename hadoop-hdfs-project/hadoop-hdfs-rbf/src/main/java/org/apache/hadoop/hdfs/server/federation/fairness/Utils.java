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

package org.apache.hadoop.hdfs.server.federation.fairness;

/**
 * Utils holds shared/utility functions for fairness classes.
 */
public class Utils {

    /**
     * getNsId extracts name service from the given namenode string.
     * An example namenode value is 'ns0.nn1'.
     *
     * @param namenode a namenode string consisting of ns ID and namenode hostname.
     * @return the extracted ns ID or empty string if input is invalid.
     */
    public static String getNsId (String namenode) {
        if (namenode == null || namenode.equals(""))
            return "";
        String[] namenodeSplit = namenode.split("\\.");
        if (namenodeSplit.length == 2) {
            return namenodeSplit[0];
        } else if (namenodeSplit.length == 1) {
            return namenode;
        }
        return "";
    }
}
