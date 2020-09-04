package org.apache.hadoop.hdfs.server.federation.store.protocol.impl.pb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRemoteLocationResponseProto;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.GetRemoteLocationResponseProtoOrBuilder;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos.RemoteLocationProto;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetRemoteLocationResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;

import com.google.protobuf.Message;

public class GetRemoteLocationResponsePBImpl extends GetRemoteLocationResponse implements PBRecord {

    private FederationProtocolPBTranslator<GetRemoteLocationResponseProto,
            GetRemoteLocationResponseProto.Builder,
            GetRemoteLocationResponseProtoOrBuilder> translator =
            new FederationProtocolPBTranslator<GetRemoteLocationResponseProto,
                    GetRemoteLocationResponseProto.Builder,
                    GetRemoteLocationResponseProtoOrBuilder>(
                    GetRemoteLocationResponseProto.class);

    public GetRemoteLocationResponsePBImpl() {
    }

    public GetRemoteLocationResponsePBImpl(
            GetRemoteLocationResponseProto proto) {
        this.translator.setProto(proto);
    }

    @Override
    public GetRemoteLocationResponseProto getProto() {
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
    public List<RemoteLocation> getRemoteLocation() throws IOException {
        List<RemoteLocationProto> remoteLocationProtos =
                this.translator.getProtoOrBuilder().getRemoteLocationList();
        List<RemoteLocation> ret = new ArrayList<RemoteLocation>();
        for (RemoteLocationProto remoteLocationProto : remoteLocationProtos) {
            RemoteLocation remoteLocation = new RemoteLocation(
                remoteLocationProto.getNameserviceId(),
                remoteLocationProto.getPath()
            );
            ret.add(remoteLocation);
        }
        return ret;
    }

    @Override
    public void setRemoteLocation(List<RemoteLocation> remoteLocations) throws IOException {
        this.translator.getBuilder().clearRemoteLocation();
        RemoteLocationProto.Builder builder = RemoteLocationProto.newBuilder();
        for (RemoteLocation remoteLocation : remoteLocations) {
            builder.clear();
            builder.setNameserviceId(remoteLocation.getNameserviceId());
            builder.setPath(remoteLocation.getDest());
            RemoteLocationProto remoteLocationProto = builder.build();
            this.translator.getBuilder().addRemoteLocation(remoteLocationProto);
        }
    }
}
