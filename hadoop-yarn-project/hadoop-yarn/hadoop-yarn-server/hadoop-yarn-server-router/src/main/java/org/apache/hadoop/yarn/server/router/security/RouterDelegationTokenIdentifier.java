package org.apache.hadoop.yarn.server.router.security;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;

public class RouterDelegationTokenIdentifier extends YARNDelegationTokenIdentifier {
    public static final Text KIND_NAME =
            new Text("RM_DELEGATION_TOKEN");

    public RouterDelegationTokenIdentifier() {
    }

    /**
     * Create a new delegation token identifier
     * @param owner the effective username of the token owner
     * @param renewer the username of the renewer
     * @param realUser the real username of the token owner
     */
    public RouterDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
        super(owner, renewer, realUser);
    }

    @Override
    public Text getKind() {
        return KIND_NAME;
    }
}
