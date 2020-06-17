package org.apache.hadoop.yarn.server.router.security;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.proto.YarnServerRouterTokenProtos.RouterDelegationTokenIdentifierDataProto;
import org.apache.hadoop.yarn.server.router.security.token.RouterDelegationTokenIdentifierData;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class RouterDTStateStoreUtils {
    public static final Log LOG = LogFactory.getLog(RouterDTStateStoreUtils.class);

    /**
     * Returns the Router Delegation Token data from the {@link DataInputStream} as a
     * {@link RouterDelegationTokenIdentifierDataProto}.  It can handle both the current
     * and old (non-protobuf) formats.
     *
     * @param fsIn The {@link DataInputStream} containing Router Delegation Token data
     * @return An {@link RouterDelegationTokenIdentifierData} containing the read in
     * Router Delegation Token
     */
    public static RouterDelegationTokenIdentifierData readRouterDelegationTokenIdentifierData(DataInputStream fsIn)
            throws IOException {
        RouterDelegationTokenIdentifierData identifierData =
                new RouterDelegationTokenIdentifierData();
        try {
            identifierData.readFields(fsIn);
        } catch (InvalidProtocolBufferException e) {
            LOG.warn("Recovering old formatted token");
            fsIn.reset();
            YARNDelegationTokenIdentifier identifier =
                    new RouterDelegationTokenIdentifier();
            identifier.readFieldsInOldFormat(fsIn);
            identifierData.setIdentifier(identifier);
            identifierData.setRenewDate(fsIn.readLong());
        }
        return identifierData;
    }

    public static RouterDelegationTokenIdentifierData getRouterDelegationTokenIdentifierData(byte[] data)
            throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        DataInputStream din = new DataInputStream(bin);
        RouterDelegationTokenIdentifierData identifierData =
                readRouterDelegationTokenIdentifierData(din);
        return identifierData;
    }
}
