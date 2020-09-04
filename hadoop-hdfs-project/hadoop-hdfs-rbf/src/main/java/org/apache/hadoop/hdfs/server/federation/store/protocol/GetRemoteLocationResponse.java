package org.apache.hadoop.hdfs.server.federation.store.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

import java.io.IOException;
import java.util.List;

/**
 * API response for getting destination path for source path.
 */
public abstract class GetRemoteLocationResponse {

    public static GetRemoteLocationResponse newInstance() throws IOException {
        return StateStoreSerializer.newRecord(GetRemoteLocationResponse.class);
    }

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract List<RemoteLocation> getRemoteLocation() throws IOException;

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract void setRemoteLocation(List<RemoteLocation> entries)
            throws IOException;
}