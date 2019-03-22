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

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_STARTUP_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.service.CompositeService.CompositeServiceShutdownHook;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Tool to start the {@link Router} for Router-based federation.
 */
public final class DFSRouter {

  private static final Logger LOG = LoggerFactory.getLogger(DFSRouter.class);


  /** Usage string for help message. */
  private static final String USAGE =
      "Usage: hdfs dfsrouter [-regular | -observer]\n" +
      "     -regular    : Normal router startup (default).\n" +
      "     -observer   : Start router in read only cluster where\n" +
      "                   the router only monitors the observer namenode.\n";

  /** Priority of the Router shutdown hook. */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;


  private DFSRouter() {
    // This is just a class to trigger the Router
  }

  /**
   * Main run loop for the router.
   *
   * @param argv parameters.
   */
  public static void main(String[] argv) {
    if (DFSUtil.parseHelpArgument(argv, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(Router.class, argv, LOG);

      Router router = new Router();

      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(router), SHUTDOWN_HOOK_PRIORITY);

      Configuration conf = new HdfsConfiguration();
      if (!parseArguments(argv, conf)) {
        printUsage(System.err);
        return;
      }

      router.init(conf);
      router.start();
    } catch (Throwable e) {
      LOG.error("Failed to start router", e);
      terminate(1, e);
    }
  }

  @VisibleForTesting
  static boolean parseArguments(String args[], Configuration conf) {
    StartupOption startOpt = StartupOption.REGULAR;
    int i = 0;

    if (args != null && args.length != 0) {
      String cmd = args[i++];
      if (StartupOption.OBSERVER.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.OBSERVER;
      } else {
        return false;
      }
    }

    setStartupOption(conf, startOpt);
    // make sure there is only one cmd
    return (args == null || i == args.length);
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_ROUTER_STARTUP_KEY, opt.toString());
  }

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }
}