package org.apache.hadoop.yarn.server.router.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.RouterAdminClient;
import org.apache.hadoop.yarn.server.router.RouterConfigKeys;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonNodeLabelManager;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonZKConfManager;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKConf;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class RouterAdminCLI extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(RouterAdminCLI.class);

  private RouterAdminClient client;

  public static void main(String[] argv) throws Exception {
    Configuration conf = new YarnConfiguration();
    RouterAdminCLI admin = new RouterAdminCLI(conf);

    int res = ToolRunner.run(admin, argv);
    System.exit(res);
  }

  public RouterAdminCLI(Configuration conf) {
    super(conf);
  }

  /**
   * Print the usage message.
   */
  public void printUsage() {
    String usage = "Router Admin Tools:\n"
        + "\t Router operation \n"
        + "\t PelotonZK Usage: yarn routeradmin -pelotonNodeLabel\n"
        + "\t[-set <node_label>]\n"
        + "\t[-get]\n"
        + "\t[-remove]\n"
        + "\t PelotonZK Usage: yarn routeradmin -pelotonZK\n"
        + "\t[-batchCreateJson \"{cluster: <cluster_name>, zkList: [{zone: <zone>, region: <region>, zk: <zk_address>, resPool: <resource_pool_path>}]}\"\n"
        + "\t[-batchUpdateJson \"{cluster: <cluster_name>, zkList: [{zone: <zone>, region: <region>, zk: <zk_address>, resPool: <resource_pool_path>}]}\"\n"
        + "\t[-remove --cluster <cluster_name>]\n"
        + "\t[-addZK --cluster <cluster_name> --zkJson \"{zone: <zone>, region: <region>, zk:<zk_address>, resPool: <resource_pool_path>}\"\n"
        + "\t[-updateZK --cluster <cluster_name> --zkJson \"{zone: <zone>, region: <region>, zk:<zk_address>, resPool: <resource_pool_path>}\"\n"
        + "\t[-rmZK --cluster <cluster_name> --zkAddress <zkAddress>\n"
        + "\t[-listZK --cluster <cluster_name>\n"
        + "\t[-clearAll\n"
        + "\t[-list\n";

    System.out.println(usage);
  }
  @Override
  public int run(String[] argv) throws Exception {
    if (argv.length < 1) {
      System.err.println("Not enough parameters specificed");
      printUsage();
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    if ("-pelotonZK".equalsIgnoreCase(cmd) || "-pelotonNodeLabel".equalsIgnoreCase(cmd)) {
      if (argv.length < 2) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    }
    boolean isPelotonZK = false;
    boolean isPelotonNodeLabel = false;
    if ("-pelotonZK".equalsIgnoreCase(cmd)) {
      isPelotonZK = true;
    } else if ("-pelotonNodeLabel".equalsIgnoreCase(cmd)) {
      isPelotonNodeLabel = true;
    }
    cmd = argv[i++];
    if (isPelotonZK && ("-batchCreateJson".equalsIgnoreCase(cmd)
        || "-batchUpdateZKConf".equalsIgnoreCase(cmd)
        || "-batchUpdateJson".equalsIgnoreCase(cmd)
        || "-remove".equalsIgnoreCase(cmd)
        || "-addZK".equalsIgnoreCase(cmd)
        || "-updateZK".equalsIgnoreCase(cmd)
        || "-rmZK".equalsIgnoreCase(cmd)
        || "-listZK".equalsIgnoreCase(cmd))) {
      if (argv.length < 3) {
        System.err.println("Not enough parameters specificed for cmd " + cmd);
        printUsage();
        return exitCode;
      }
    }

    // Initialize RouterClient
    try {
      String address = getConf().getTrimmed(
          RouterConfigKeys.ROUTER_ADMIN_ADDRESS_KEY,
          RouterConfigKeys.ROUTER_ADMIN_ADDRESS_DEFAULT);
      InetSocketAddress routerSocket = NetUtils.createSocketAddr(address);
      client = new RouterAdminClient(routerSocket, getConf());
    } catch (RPC.VersionMismatch v) {
      System.err.println(
          "Version mismatch between client and server... command aborted");
      return exitCode;
    } catch (IOException e) {
      System.err.println("Bad connection to Router... command aborted");
      return exitCode;
    }

    Exception debugException = null;
    exitCode = 0;
    try {
      if (isPelotonZK) {
        if ("-batchCreateJson".equalsIgnoreCase(cmd)) {
          if (batchCreatePelotonZKConf(argv, i, true)) {
            System.out.println("Successfuly create Peloton ZKConf " + argv[i]);
          }
        } else if ("-batchUpdateJson".equalsIgnoreCase(cmd)) {
          if (batchCreatePelotonZKConf(argv, i, false)) {
            System.out.println("Successfuly update Peloton ZKConf " + argv[i]);
          }
        } else if ("-remove".equalsIgnoreCase(cmd)) {
          if (removeZKConfByCluster(argv, i)) {
            System.out.println("Successfuly remove Peloton ZKConf " + argv[i]);
          }
        } else if ("-addZK".equalsIgnoreCase(cmd)) {
          if (saveZKInfoToCluster(argv, i, true)) {
            System.out.println("Successfuly add Peloton zk info " + argv[i]);
          }
        } else if ("-updateZK".equalsIgnoreCase(cmd)) {
          if (saveZKInfoToCluster(argv, i, false)) {
            System.out.println("Successfuly update Peloton zk info " + argv[i]);
          }
        } else if ("-rmZK".equalsIgnoreCase(cmd)) {
          if (removeZKInfoFromCluster(argv, i)) {
            System.out.println("Successfuly remove Peloton zkInfo" + argv[i]);
          }
        } else if ("-listZK".equalsIgnoreCase(cmd)) {
          getZKInfoList(argv, i);
        } else if ("-clearAll".equalsIgnoreCase(cmd)) {
          clearAll();
        } else if ("-list".equalsIgnoreCase(cmd)) {
          getPelotonZKConfList();
        } else {
          printUsage();
          return exitCode;
        }
      } else if (isPelotonNodeLabel) {
        if ("-get".equalsIgnoreCase(cmd)) {
          getPelotonNodeLabel();
        } else if ("-set".equalsIgnoreCase(cmd)) {
          if (savePelotonNodeLabel(argv, i)) {
            System.out.println("Successfuly save Peloton node label" + argv[i]);
          } else {
            System.out.println("Failed to save peloton node label");
          }
        } else if ("-remove".equalsIgnoreCase(cmd)) {
          if (removePelotonNodeLabel()) {
            System.out.println("Successfuly remove Peloton node label");
          } else {
            System.out.println("Failed to remove peloton node label");
          }
        } else {
          printUsage();
          return exitCode;
        }
      }

    } catch (IllegalArgumentException arge) {
      debugException = arge;
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage();
    } catch (Exception e) {
      exitCode = -1;
      debugException = e;
      System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage());
      e.printStackTrace();
    }
    if (debugException != null) {
      LOG.debug("Exception encountered", debugException);
    }
    return exitCode;
  }

  public boolean batchCreatePelotonZKConf(String[] params, int i, boolean isCreate) throws IOException, JSONException {
    String target = params[i++];
    JSONObject obj = new JSONObject(target);
    if (!obj.has("cluster")) {
      throw new IOException("Missing cluster");
    }
    String cluster = obj.getString("cluster");
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    List<PelotonZKInfo> zkInfoObjs;
    StringBuilder errorMsg = new StringBuilder();
    if (!obj.has("zkList")) {
      zkInfoObjs = new ArrayList<>();
    } else {
      JSONArray jsonArray = obj.getJSONArray("zkList");
      zkInfoObjs = new ArrayList<>(jsonArray.length());
      for(int idx = 0; idx< jsonArray.length(); idx++) {
        String zone = jsonArray.getJSONObject(idx).getString("zone");
        String region = jsonArray.getJSONObject(idx).getString("region");
        String zkAddress = jsonArray.getJSONObject(idx).getString("zk");
        String resPool = jsonArray.getJSONObject(idx).getString("resPool");
        if (zone == null || zone.isEmpty() || region == null || region.isEmpty()
          || zkAddress == null || zkAddress.isEmpty() || resPool == null || resPool.isEmpty()) {
          errorMsg.append(idx).append(" zk conf is missing parameters");
          continue;
        }
        System.out.println(String.format(
            "Adding to cluster %s with zone: %s, region: %s, zkAddress: %s, resPool: %s",
          cluster, zone, region, zkAddress, resPool));
        zkInfoObjs.add(PelotonZKInfo.newInstance(zone, region, zkAddress, resPool));
      }
    }
    SavePelotonZKConfRequest request = SavePelotonZKConfRequest.newInstance(PelotonZKConf.newInstance(
        cluster, zkInfoObjs
    ), isCreate);
    SavePelotonZKConfResponse response = pelotonZKConfManager.savePelotonZKConf(request);
    boolean status = response.getStatus();
    if (!status) {
      System.err.println("cannot create Peloton ZKConf with cluster " + cluster);
    }
    if (errorMsg.length() > 0) {
      System.err.println("failed to create " + errorMsg.toString());
    }
    return status;
  }

  public boolean saveZKInfoToCluster(String[] params, int i, boolean isCreate) throws IOException, JSONException {
    if (params.length < 6) {
      System.err.println("Missing params.");
      return false;
    }
    if (!params[i++].equalsIgnoreCase("--cluster")) {
      System.err.println("Missing --cluster");
      return false;
    }
    String cluster = params[i++];
    if (cluster == null || cluster.isEmpty()) {
      System.err.println("cluster cannot be empty or null");
      return false;
    }
    if (!params[i++].equalsIgnoreCase("--zkJson")) {
      System.err.println("Missing --zkJson");
      return false;
    }
    String zkInfoJsonStr = params[i];
    System.out.println("zkInfoJsonStr: " + zkInfoJsonStr);
    JSONObject zkInfObj = new JSONObject(zkInfoJsonStr);
    String zone = zkInfObj.getString("zone");
    String region = zkInfObj.getString("region");
    String zkAddress = zkInfObj.getString("zk");
    String resPool = zkInfObj.getString("resPool");
    if (zone == null || region == null || zkAddress == null || zkAddress.isEmpty()
      || resPool == null || resPool.isEmpty()) {
      System.err.println(" zk conf is missing parameters");
      return false;
    }
    System.out.println(String.format(
        "Adding to cluster %s with zone: %s, region: %s, zkAddress: %s",
        cluster, zone, region, zkAddress));
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    PelotonZKInfo zkInfo = PelotonZKInfo.newInstance(zone, region, zkAddress, resPool);
    SavePelotonZKInfoToClusterRequest request = SavePelotonZKInfoToClusterRequest.newInstance(
        cluster, zkInfo, isCreate
    );
    SavePelotonZKInfoToClusterResponse response = pelotonZKConfManager.savePelotonZKInfoToCluster(request);
    boolean status = response.getStatus();
    String errorMsg = response.getErrorMessage();
    if (!status){
      System.err.println("Failed to add zkInfo due to "+ errorMsg);
    }
    return status;
  }

  public boolean removeZKInfoFromCluster(String[] params, int i) throws IOException {
    if (params.length < 6) {
      System.err.println("Missing params.");
      return false;
    }
    if (!params[i++].equalsIgnoreCase("--cluster")) {
      System.err.println("Missing --cluster");
      return false;
    }
    String cluster = params[i++];
    if (cluster == null || cluster.isEmpty()) {
      System.err.println("cluster cannot be empty or null");
      return false;
    }
    if (!params[i++].equalsIgnoreCase("--zkAddress")) {
      System.err.println("Missing --zkAddress");
      return false;
    }
    String zkAddress = params[i];
    if (zkAddress == null || zkAddress.isEmpty()) {
      System.err.println("cluster cannot be empty or null");
      return false;
    }
    System.out.println("removeZKInfoFromCluster : cluster : " + cluster + " zk: " + zkAddress);
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    RemovePelotonZKInfoFromClusterRequest request = RemovePelotonZKInfoFromClusterRequest.newInstance(
        cluster, zkAddress
    );
    RemovePelotonZKInfoFromClusterResponse response = pelotonZKConfManager.removePelotonZKInfoFromCluster(request);
    boolean isRemoved = response.getStatus();
    if (!isRemoved) {
      System.err.println("cannot remove ZK info from cluster: " + cluster + " with zkAddress: " + zkAddress);
      System.err.println("error: " + response.getErrorMessage());
    }
    return isRemoved;
  }

  public boolean removeZKConfByCluster(String[] params, int i) throws IOException {
    if (params.length < 4) {
      System.err.println("Missing params.");
      return false;
    }
    if (!params[i++].equalsIgnoreCase("--cluster")) {
      System.err.println("Missing --cluster");
      return false;
    }
    String cluster = params[i++];
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    RemovePelotonZKConfByClusterRequest request = RemovePelotonZKConfByClusterRequest.newInstance(cluster);
    RemovePelotonZKConfByClusterResponse response = pelotonZKConfManager.removePelotonZKConfByCluster(request);
    boolean removed = response.getStatus();
    if (!removed) {
      System.err.println("cannot add Peloton zk configuration with cluster " + cluster);
    }
    return removed;
  }

  public void getZKInfoList(String[] params, int i) throws IOException {
    if (params.length < 4) {
      System.err.println("Missing params.");
      return;
    }
    if (!params[i++].equalsIgnoreCase("--cluster")) {
      System.err.println("Missing --cluster");
      return;
    }
    String cluster = params[i++];
    if (cluster == null || cluster.isEmpty()) {
      System.err.println("cluster cannot be empty or null");
      return;
    }
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    GetPelotonZKInfoListByClusterResponse response = pelotonZKConfManager.getPelotonZKConfListByCluster(
        GetPelotonZKInfoListByClusterRequest.newInstance(cluster)
    );
    List<PelotonZKInfo> results = response.getPelotonZKInfoList();
    System.out.println("Peloton ZK List in cluster: " + cluster);
    System.out.println(String.format(
        "%-25s %-25s %-25s %-25s",
        "zkAddress", "resPool", "zone", "region"));
    for(PelotonZKInfo zkInfo: results) {
      String zone = zkInfo.getZone();
      String region = zkInfo.getRegion();
      String zkAddress = zkInfo.getZKAddress();
      String resPool = zkInfo.getResourcePoolPath();
      System.out.print(String.format("%-25s %-25s %-25s %-25s\n",
          zkAddress, resPool, zone, region));
    }
  }

  public void clearAll() throws IOException {
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    ClearAllPelotonZKConfsResponse response = pelotonZKConfManager.clearAllPelotonZKConfs(
        ClearAllPelotonZKConfsRequest.newInstance()
    );
    response.getStatus();
  }

  public void getPelotonZKConfList() throws IOException {
    PelotonZKConfManager pelotonZKConfManager = client.getPelotonZKConfManager();
    GetPelotonZKConfListResponse response = pelotonZKConfManager.getPelotonZKConfList(
        GetPelotonZKConfListRequest.newInstance()
    );
    List<PelotonZKConf> results =  response.getPelotonZKConfList();
    System.out.println("Peloton ZK configuration:");
    for(PelotonZKConf zkConf: results) {
      String cluster = zkConf.getCluster();
      System.out.println("cluster: " + cluster);
      System.out.println(String.format(
          "%-25s %-25s %-25s",
          "zkAddress", "zone", "region"));
      for (PelotonZKInfo zkInfo: zkConf.getPelotonZKInfoList()) {
        System.out.print(String.format("%-25s %-25s %-25s\n",
            zkInfo.getZKAddress(), zkInfo.getZone(), zkInfo.getRegion()));
      }
    }
  }

  public void getPelotonNodeLabel() throws IOException {
    PelotonNodeLabelManager pelotonNodeLabelManager = client.getPelotonNodeLabelManager();
    GetPelotonNodeLabelResponse response = pelotonNodeLabelManager.getPelotonNodeLabel(
        GetPelotonNodeLabelRequest.newInstance()
    );
    if (response == null) {
      System.out.println("Peloton hosts node label is DEFAULT");
      return;
    }
    String result =  response.getPelotonNodeLabel();
    System.out.println("Node label for Peloton hosts is not defined. These hosts will be added in DEFAULT.");
    System.out.println(result);
  }

  public boolean savePelotonNodeLabel(String[] params, int i) throws IOException {
    String label = params[i++];
    if (label == null) {
      System.err.println("label cannot be null");
      return false;
    }
    PelotonNodeLabelManager pelotonNodeLabelManager = client.getPelotonNodeLabelManager();
    SavePelotonNodeLabelResponse response = pelotonNodeLabelManager.savePelotonNodeLabel(
        SavePelotonNodeLabelRequest.newInstance(label)
    );
    return response.getStatus();
  }

  public boolean removePelotonNodeLabel() throws IOException {
    PelotonNodeLabelManager pelotonNodeLabelManager = client.getPelotonNodeLabelManager();
    SavePelotonNodeLabelResponse response = pelotonNodeLabelManager.savePelotonNodeLabel(
        SavePelotonNodeLabelRequest.newInstance("")
    );
    return response.getStatus();
  }
}

