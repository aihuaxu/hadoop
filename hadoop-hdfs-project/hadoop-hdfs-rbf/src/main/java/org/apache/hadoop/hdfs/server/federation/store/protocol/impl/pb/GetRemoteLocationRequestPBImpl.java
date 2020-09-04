package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import java.io.IOException;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRemoteLocationRequestProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRemoteLocationRequestProtoOrBuilder;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRemoteLocationRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import com.google.protobuf.Message;

public class GetRemoteLocationRequestPBImpl  extends GetRemoteLocationRequest implements PBRecord {
    private FederationProtocolPBTranslator<GetRemoteLocationRequestProto,
            GetRemoteLocationRequestProto.Builder,
            GetRemoteLocationRequestProtoOrBuilder> translator =
            new FederationProtocolPBTranslator<GetRemoteLocationRequestProto,
                    GetRemoteLocationRequestProto.Builder,
                    GetRemoteLocationRequestProtoOrBuilder>(
                    GetRemoteLocationRequestProto.class);

    public GetRemoteLocationRequestPBImpl() {
    }

    public GetRemoteLocationRequestPBImpl(
            GetRemoteLocationRequestProto proto) {
        this.translator.setProto(proto);
    }

    @Override
    public GetRemoteLocationRequestProto getProto() {
        return this.translator.build();
    }

    @Override
    public void setProto(Message proto) {
        this.translator.setProto(proto);
    }

    @Override
    public void readInstance(String base64String) throws IOException {
        this.translator.readInstance(base64String);
    }

    @Override
    public String getSrcPath() {
        return this.translator.getProtoOrBuilder().getSrcPath();
    }

    @Override
    public void setSrcPath(String path) {
        this.translator.getBuilder().setSrcPath(path);
    }
}
