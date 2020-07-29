package org.apache.hadoop.yarn.server.router.external.yarnonpeloton;

import com.uber.peloton.client.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peloton.api.v0.respool.ResourceManagerGrpc;
import peloton.api.v0.respool.Respool;

/**
 * This class provides functions to call Peloton APIs for get hosts, create jobs, etc
 */
public class PelotonHelper {
  private final static Logger LOG =
    LoggerFactory.getLogger(PelotonHelper.class);

  private static String PELOTON_ZK = "zookeeper-mesos-devel01-phx3.uber.internal:2181/peloton";
  private static String CLIENT_NAME = "Yarn Router";

  protected void initialize() {
    // TODO: initialize peloton service clients like JobService
  }

  protected void lookupPool() {
    // YoP does not need this API, this is to demonstrate Peloton connection from YARN Router
    ResourceManagerGrpc.ResourceManagerBlockingStub resourceManager =
      new ResourceManager(CLIENT_NAME, PELOTON_ZK)
      .blockingConnect();

    String pool = "/StatelessResPool";
    Respool.LookupRequest request = Respool.LookupRequest.newBuilder()
      .setPath(Respool.ResourcePoolPath.newBuilder()
        .setValue(pool)
        .build())
      .build();

    Respool.LookupResponse response = resourceManager.lookupResourcePoolID(request);
    LOG.info(String.format("Looked up pool successfully: name=%s, id=%s", pool, response.getId().toString()));
  }

  public static void main(String[] args) {
    // Test Peloton connection, not from router process
    LOG.info("Test Peloton connection without security, not from router process");
    new PelotonHelper().lookupPool();
  }
}
