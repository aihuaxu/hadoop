package org.apache.hadoop.fs.contract.router.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import static org.junit.Assert.assertEquals;

/**
 * Test getresolvepath operation through Router web HDFS HTTP request.
 */
public class TestRouterWebHDFSContractResolvePath {

    private static FileSystem webHdfsFs;

    @BeforeClass
    public static void setupCluster() throws Exception {
        RouterWebHDFSContract.createCluster();
        webHdfsFs = RouterWebHDFSContract.getFileSystem();
    }

    @AfterClass
    public static void teardownCluster() throws IOException {
        RouterWebHDFSContract.destroyCluster();
    }

    @Test
    public void testGetResolvePathWithMountPoint() throws IOException {
        compareResolvePath("hdfs://ns1/target-ns1", "/ns1");
    }

    @Test
    public void testGetResolvePathWithoutMountPoint() throws IOException {
        // expect to resolve with default ns.
        compareResolvePath("hdfs://ns0/no-entry", "/no-entry");
    }

    private void compareResolvePath(String expectResolvePath, String testPath) throws IOException{
        URI addr = webHdfsFs.getUri();
        URL url = new URL("http", addr.getHost(), addr.getPort(),
                WebHdfsFileSystem.PATH_PREFIX + testPath + "?" + GetOpParam.Op.GETRESOLVEPATH.toQueryString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        int code = conn.getResponseCode();
        assertEquals(200, code);
        String resp = getResponseOutput(conn);
        assertEquals(expectResolvePath, resp);
    }

    private String getResponseOutput(HttpURLConnection conn) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                conn.getInputStream()));
        String line;
        StringBuffer response = new StringBuffer();

        while ((line = in.readLine()) != null) {
            response.append(line);
        }
        in.close();
        return response.toString();
    }
}
