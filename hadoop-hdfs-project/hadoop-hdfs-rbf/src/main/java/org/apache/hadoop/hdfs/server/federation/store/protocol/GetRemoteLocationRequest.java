package org.apache.hadoop.hdfs.server.federation.store.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

import java.io.IOException;

/**
 * API request for getting destination path for source path.
 */
public abstract class GetRemoteLocationRequest {
    public static GetRemoteLocationRequest newInstance() throws IOException {
        return StateStoreSerializer.newRecord(GetRemoteLocationRequest.class);
    }

    public static GetRemoteLocationRequest newInstance(String srcPath)
            throws IOException {
        GetRemoteLocationRequest request = newInstance();
        request.setSrcPath(srcPath);
        return request;
    }

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract String getSrcPath();

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract void setSrcPath(String path);
}
