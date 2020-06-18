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
package org.apache.hadoop.hdfs.server.namenode.web.resources;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.HttpMethod;

/**
 * Test WebHDFS SkipTrash feature.
 */

public class TestWebHDFSSkipTrash {

  private static MiniDFSCluster cluster;
  private static final String USER = System.getProperty("user.name");
  private static final String FILE_PATH = "/test-file";
  private static final String DIRECTORY_PATH = "/test-directory";
  private static final String TRASH_FILE_PATH = "/user/" + USER +
      "/.Trash/Current" + FILE_PATH;
  private static final String TRASH_DIRECTORY_PATH = "/user/" + USER +
      "/.Trash/Current" + DIRECTORY_PATH;

  private static final String GETFILESTATUS = "op=GETFILESTATUS";
  private static final String DELETE = "op=DELETE";
  private static final String RECURSIVE = "recursive=true";

  @BeforeClass
  public static void initializeMiniDFSCluster() throws Exception {
    Configuration conf = WebHdfsTestUtil.createConf();
    conf.setLong(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 30);
    cluster = new MiniDFSCluster.Builder(conf).build();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUpBefore() throws IOException {
    createDirectory();
    createFile();
  }

  @After
  public void cleanUpAfter() throws IOException {
    deleteTrash();
  }

  @Test
  public void testMoveFileToTrash() throws IOException {
    HttpURLConnection connection = getURLConnection(HttpMethod.DELETE,
        FILE_PATH, DELETE, RECURSIVE, "skiptrash=false");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, FILE_PATH, GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, TRASH_FILE_PATH,
        GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
  }

  @Test
  public void testMoveDirectoryToTrash() throws IOException {
    HttpURLConnection connection = getURLConnection(HttpMethod.DELETE,
        DIRECTORY_PATH, DELETE, RECURSIVE, "skiptrash=false");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, DIRECTORY_PATH,
        GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, TRASH_DIRECTORY_PATH,
        GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
  }

  @Test
  public void testSkipFileToTrash() throws IOException {
    HttpURLConnection connection = getURLConnection(HttpMethod.DELETE,
        FILE_PATH, DELETE, RECURSIVE, "skiptrash=true");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, FILE_PATH, GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, TRASH_FILE_PATH,
        GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        connection.getResponseCode());
  }

  @Test
  public void testSkipDirectoryToTrash() throws IOException {
    HttpURLConnection connection = getURLConnection(HttpMethod.DELETE,
        DIRECTORY_PATH, DELETE, RECURSIVE, "skiptrash=true");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, DIRECTORY_PATH,
        GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        connection.getResponseCode());
    connection = getURLConnection(HttpMethod.GET, TRASH_DIRECTORY_PATH,
        GETFILESTATUS);
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND,
        connection.getResponseCode());
  }

  private HttpURLConnection getURLConnection(String requestMethod, String path,
      String... params) {
    HttpURLConnection connection = null;
    final StringBuilder uri = new StringBuilder(cluster.getHttpUri(0));
    uri.append("/webhdfs/v1").
        append(path).
        append("?user.name=").
        append(USER).
        append("&");
    for (String param : params) {
      uri.append(param).append("&");
    }

    try {
      URL url = new URL(uri.toString());
      connection = (HttpURLConnection) url.openConnection();
      if (requestMethod.equals(HttpMethod.PUT)) {
        connection.setRequestMethod(HttpMethod.PUT);
      } else if (requestMethod.equals(HttpMethod.DELETE)) {
        connection.setRequestMethod(HttpMethod.DELETE);
      } else {
        connection.setRequestMethod(HttpMethod.GET);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return connection;
  }

  private void createFile() throws IOException {
    String option = "OP=CREATE";
    HttpURLConnection connection = getURLConnection(HttpMethod.PUT,
        FILE_PATH, option, "overwrite=true");
    Assert.assertEquals(HttpURLConnection.HTTP_CREATED,
        connection.getResponseCode());
  }

  private void createDirectory() throws IOException {
    String option = "OP=MKDIRS";
    HttpURLConnection connection = getURLConnection(HttpMethod.PUT,
        DIRECTORY_PATH, option, "overwrite=true");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
  }

  private void deleteTrash() throws IOException {
    String option = "OP=DELETE";
    HttpURLConnection connection = getURLConnection(HttpMethod.DELETE,
        TRASH_FILE_PATH, option, "skiptrash=true", "recursive=true");
    Assert.assertEquals(HttpURLConnection.HTTP_OK,
        connection.getResponseCode());
  }
}