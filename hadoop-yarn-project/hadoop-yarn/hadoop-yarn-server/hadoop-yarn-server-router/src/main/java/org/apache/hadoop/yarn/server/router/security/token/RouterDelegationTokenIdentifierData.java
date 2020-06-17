package org.apache.hadoop.yarn.server.router.security.token;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos;
import org.apache.hadoop.yarn.proto.YarnServerRouterTokenProtos.RouterDelegationTokenIdentifierDataProto;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenIdentifier;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;


public class RouterDelegationTokenIdentifierData {
    RouterDelegationTokenIdentifierDataProto.Builder builder =
            RouterDelegationTokenIdentifierDataProto.newBuilder();

    public RouterDelegationTokenIdentifierData() {}

    public RouterDelegationTokenIdentifierData(
            YARNDelegationTokenIdentifier identifier, long renewdate, String password) {
        builder.setTokenIdentifier(identifier.getProto());
        builder.setRenewDate(renewdate);
        builder.setPassword(password);
    }

    public RouterDelegationTokenIdentifierData(
            YARNDelegationTokenIdentifier identifier, long renewdate, byte[] password) {
        builder.setTokenIdentifier(identifier.getProto());
        builder.setRenewDate(renewdate);
        builder.setPassword(new String(Base64.encodeBase64(password)));
    }

    public void readFields(DataInput in) throws IOException {
        builder.mergeFrom((DataInputStream) in);
    }

    public void write(DataOutput out) throws IOException {
        builder.build().writeTo((DataOutputStream) out);
    }

    public byte[] toByteArray() {
        return builder.build().toByteArray();
    }

    public RouterDelegationTokenIdentifier getTokenIdentifier() throws IOException {
        YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto yarnIdentProto = builder.getTokenIdentifier();
        ByteArrayInputStream in =
                new ByteArrayInputStream(yarnIdentProto.toByteArray());
        RouterDelegationTokenIdentifier identifer = new RouterDelegationTokenIdentifier();
        identifer.readFields(new DataInputStream(in));
        return identifer;
    }

    public long getRenewDate() {
        return builder.getRenewDate();
    }

    public byte[] getPassword() {
        String password = builder.getPassword();
        return Base64.decodeBase64(password.getBytes());
    }

    public void setIdentifier(YARNDelegationTokenIdentifier identifier) {
        builder.setTokenIdentifier(identifier.getProto());
    }

    public void setRenewDate(long renewDate) {
        builder.setRenewDate(renewDate);
    }

    public void setPassword(String password) {
        builder.setPassword(password);
    }
}
