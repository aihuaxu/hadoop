package org.apache.hadoop.yarn.server.router.external.peloton;

import com.peloton.api.v0.host.svc.pb.HostSvc.QueryHostsRequest;
import com.peloton.api.v0.host.svc.pb.HostSvc.QueryHostsResponse;
import com.peloton.api.v0.host.svc.pb.HostSvc.SetReclaimHostOrderRequest;
import com.peloton.api.v0.host.svc.pb.HostServiceGrpc.HostServiceBlockingStub;
import com.peloton.api.v0.host.svc.pb.HostSvc.ListHostPoolsRequest;
import com.peloton.api.v0.host.svc.pb.HostSvc.ListHostPoolsResponse;
import com.uber.peloton.client.HostManager;
import com.uber.peloton.client.ResourceManager;
import com.uber.peloton.client.StatelessJobService;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.metrics.YoPMetrics;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peloton.api.v0.host.Host.HostInfo;
import peloton.api.v0.host.Host.HostPoolInfo;
import peloton.api.v0.host.Host.HostState;
import peloton.api.v0.respool.ResourceManagerGrpc.ResourceManagerBlockingStub;
import peloton.api.v1alpha.job.stateless.Stateless;
import peloton.api.v1alpha.job.stateless.svc.JobServiceGrpc.JobServiceBlockingStub;
import peloton.api.v1alpha.job.stateless.svc.StatelessSvc;
import peloton.api.v1alpha.peloton.Peloton;
import peloton.api.v1alpha.job.stateless.Stateless.CreateSpec;
import peloton.api.v1alpha.job.stateless.Stateless.UpdateSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * This class provides functions to call Peloton APIs for get hosts, create jobs, etc
 */
public class PelotonHelper {

  private final static Logger LOG =
      LoggerFactory.getLogger(PelotonHelper.class);

  private RouterStateStoreService routerStateStore;
  private PelotonZKConfRecordStore pelotonZKConfRecordStore;
  private PelotonNodeLabelRecordStore pelotonNodeLabelRecordStore;
  private RouterClientRMService clientRMService;
  private ResourceManagerAdministrationProtocol rmAdminProxy;
  private static String CLIENT_NAME = "Yarn Router";
  private List<PelotonClientWrapper> pelotonClientWrapperList;
  private Configuration conf;
  private YoPMetrics metrics;

  //username and password for Peloton gRPC connection
  private String yarnUser = "";
  private static String yarnPassword = "";

  private Peloton.JobID pelotonJobId;
  private boolean isInitialized = false;

  public PelotonHelper(RouterStateStoreService routerStateStore, RouterClientRMService clientRMService,
      ResourceManagerAdministrationProtocol rmAdminProxy){
    this.routerStateStore = routerStateStore;
    this.clientRMService = clientRMService;
    this.rmAdminProxy = rmAdminProxy;
    this.metrics = YoPMetrics.getMetrics();
  }

  protected void getOrderedHosts() {
    if (!isInitialized) {
      LOG.warn("not initialized.");
      return;
    }
    long getHostOrderStartTime = monotonicNow();
    GetOrderedHostsRequest request = GetOrderedHostsRequest.newInstance();
    List<String> orderedList = null;
    try {
      //TODO: getOrderedHosts needs to be improved to get ordered hosts from each Peloton cluster separately
      GetOrderedHostsResponse response = clientRMService.getOrderedHosts(request);
      orderedList = response.getOrderedHosts();
      metrics.setSucceedGetOrderedHostsFromRM(monotonicNow() - getHostOrderStartTime);
    } catch (Exception e) {
      LOG.error("Error getting OrderedHosts", e);
      metrics.incrGetOrderedHostsFromRMFailure();
    }
    if (orderedList != null && !orderedList.isEmpty()) {
      for (PelotonClientWrapper clientWrapper : pelotonClientWrapperList) {
        try {
          HostServiceBlockingStub hostService = clientWrapper.getHostService();
          ListHostPoolsResponse pools = listHostsPools(hostService);
          for (HostPoolInfo poolInfo : pools.getPoolsList()) {
            if (poolInfo.getName()
              .equals(PelotonJobSpec.Constants.PELOTON_HOST_POOL_SHARED_TO_YARN)) {
              long getSetReclaimStart = monotonicNow();
              SetReclaimHostOrderRequest claimerRequest = SetReclaimHostOrderRequest.newBuilder()
                .setHostpool(poolInfo.getName())
                .addAllHosts(orderedList).build();
              hostService.setReclaimHostOrder(claimerRequest);
              LOG.info("setReclaimHostOrder call succeeded for Peloton cluster "
                + clientWrapper.getPelotonZKInfo());
              metrics.setSucceedReclaimHostOrder(monotonicNow() - getSetReclaimStart);
            }
          }
        } catch(Exception e){
          LOG.error("Failed to setReclaimHostOrder for Peloton cluster "
            + clientWrapper.getPelotonZKInfo(), e);
          metrics.incrSetReclaimHostFailure();
        }
      }
    }
  }

  protected boolean getYarnCredentialOnPeloton() {
    String yopCredentialPath = conf.get(YarnConfiguration.ROUTER_YARN_USER_ON_PELOTON_PATH,
      YarnConfiguration.DEFAULT_ROUTER_YARN_USER_ON_PELOTON_PATH);
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(yopCredentialPath));
      yarnUser = br.readLine().substring("username: ".length());
      yarnPassword = br.readLine().substring("password: ".length());
    } catch (Exception e) {
      LOG.error("Failed to load Yarn credential on Peloton from Langley", e);
      metrics.incrFetchPelotonCredstFailure();
      return false;
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch(IOException e) {
          LOG.warn("Failed to close buffer reader of Yarn credential file", e);
        }
      }
    }
    LOG.info("Got yarn credential on Peloton from langley successfully, user={}", yarnUser);
    return true;
  }

  protected void initialize(Configuration conf) {
    // initialize connections to peloton services
    if (!routerStateStore.isDriverReady()) {
      isInitialized = false;
      return;
    }

    this.conf = conf;
    if (!getYarnCredentialOnPeloton()) {
      LOG.error("Initialize failed because not able to load yarn credential on Peloton");
      return;
    };
    List<PelotonZKInfo> zkInfoList = getPelotonZkList(YarnConfiguration.getClusterId(conf));
    LOG.info("Configured Peloton clusters in Router zk: " + zkInfoList);
    if (zkInfoList != null && !zkInfoList.isEmpty()) {
      pelotonClientWrapperList = new ArrayList<>();
      for (PelotonZKInfo zkInfo: zkInfoList) {
        LOG.info("Initializing Peloton client for " + zkInfo);
        PelotonClientWrapper clientWrapper = PelotonClientWrapper.newInstance();
        if (zkInfo == null) {
          continue;
        }
        String pelotonZK = zkInfo.getZKAddress();
        clientWrapper.setPelotonZKInfo(zkInfo);
        //Create Peloton gRPC clients, will connect to gRPC servers in the monitoring thread
        clientWrapper.setHostManagerClient(new HostManager(CLIENT_NAME, pelotonZK));
        clientWrapper.getHostManagerClient().setAuth(yarnUser, yarnPassword);
        clientWrapper.setResourceManagerClient(new ResourceManager(CLIENT_NAME, pelotonZK));
        clientWrapper.getResourceManagerClient().setAuth(yarnUser, yarnPassword);
        clientWrapper.setStatelessSvcClient(new StatelessJobService(CLIENT_NAME, pelotonZK));
        clientWrapper.getStatelessSvcClient().setAuth(yarnUser, yarnPassword);
        pelotonClientWrapperList.add(clientWrapper);
        LOG.info("Initialized Peloton client successfully for " + clientWrapper.getPelotonZKInfo());
      }
      isInitialized = true;
    }
  }

  protected void connectPelotonServices() {
    if (!isInitialized) {
      LOG.warn("not initialized.");
      return;
    }
    long start = monotonicNow();
    // Call Peloton client API to connect its gRPC service,
    // Client API blockingConnect will take care of gRPC&zk timeout or failover.
    // Router can directly call this API without worrying about the connection issues.
    for (PelotonClientWrapper clientWrapper: pelotonClientWrapperList) {
      clientWrapper.setHostService(clientWrapper.getHostManagerClient().blockingConnect());
      clientWrapper.setJobSvc(clientWrapper.getStatelessSvcClient().blockingConnect());
      clientWrapper.setResourceManager(clientWrapper.getResourceManagerClient().blockingConnect());
      LOG.info("Connected to Peloton gRPC services successfully: " + clientWrapper.getPelotonZKInfo());
    }
    long end = monotonicNow();
    metrics.connectPelotonServices(end - start);
  }

  protected ListHostPoolsResponse listHostsPools(HostServiceBlockingStub hostService) {
    ListHostPoolsRequest request = ListHostPoolsRequest.newBuilder().build();
    ListHostPoolsResponse response = hostService.listHostPools(request);
    return response;
  }

  /**
   * Query only active hosts in Peloton shared host pool, to filter those hosts being evicted
   * @param hostService
   * @return list of active hosts in shared pool
   */
  protected QueryHostsResponse queryActiveHostsInSharedPool(HostServiceBlockingStub hostService) {
    QueryHostsRequest request = QueryHostsRequest.newBuilder()
      .addAllHostStates(Arrays.asList(HostState.HOST_STATE_UP))
      .addHostGoalStates(HostState.HOST_STATE_UP)
      .addAllCurrentHostPools(Arrays.asList(PelotonJobSpec.Constants.PELOTON_HOST_POOL_SHARED_TO_YARN))
      .build();
    QueryHostsResponse response = hostService.queryHosts(request);
    LOG.info("queryHosts returned {} active hosts", response.getHostInfosCount());
    return response;
  }

  protected void setJobId(Peloton.JobID jobID) {
    pelotonJobId = jobID;
  }

  protected Peloton.JobID getJobID() {
    return pelotonJobId;
  }

  /**
   * - call ListHostPools to get desired NM instance count
   * - for each pool:
   *    - call QueryJobs to check whether there is a NM job running
   *      - Yes: get existing job id, call API UpdateJob with all configuration and new instance numbers
   *      - No: call CreateJob to create a stateless job for NM with initial number of instances
   */
  protected void startNMsOnPeloton() {
    if (!isInitialized) {
      LOG.warn("not initialized.");
      return;
    }
    for (PelotonClientWrapper clientWrapper: pelotonClientWrapperList) {
      LOG.info("startNMsOnPeloton in Peloton cluster " + clientWrapper.getPelotonZKInfo());
      try {
        int instanceNumber = 0;
        QueryHostsResponse queryHostsResponse = queryActiveHostsInSharedPool(clientWrapper.getHostService());
        Set<String> hostSetToInclude = new HashSet<>();
        Map<NodeId, Set<String>> nodeLabels = new HashMap<>();
        Set<String> targetLabelSet = new HashSet<>();
        GetPelotonNodeLabelResponse getPelotonNodeLabelResponse = getPelotonNodeLabelRecordStore().getPelotonNodeLabel(
            GetPelotonNodeLabelRequest.newInstance()
        );
        if (getPelotonNodeLabelResponse == null
            || getPelotonNodeLabelResponse.getPelotonNodeLabel() == null
            || getPelotonNodeLabelResponse.getPelotonNodeLabel().isEmpty()) {
          LOG.info("Node label for Peloton hosts is not defined. These hosts will be added in "
              + NodeLabel.DEFAULT_NODE_LABEL_PARTITION);
        } else {
          String nodeLabel = getPelotonNodeLabelResponse.getPelotonNodeLabel();
          targetLabelSet.add(nodeLabel);
          LOG.info("Peloton hosts node label is " + nodeLabel);
        }
        if (queryHostsResponse.getHostInfosList() != null) {
          instanceNumber = queryHostsResponse.getHostInfosCount();
          for (HostInfo hostInfo : queryHostsResponse.getHostInfosList()) {
            String hostName = hostInfo.getHostname();
            NodeId nodeId = ConverterUtils.toNodeIdWithDefaultPort(hostName);
            hostSetToInclude.add(hostInfo.getHostname());
            nodeLabels.put(nodeId, targetLabelSet);
          }
          LOG.debug("Peloton {} pool has {} active hosts for YARN: {}",
            PelotonJobSpec.Constants.PELOTON_HOST_POOL_SHARED_TO_YARN, instanceNumber, hostSetToInclude);
          try {
            IncludeExternalHostsRequest request = IncludeExternalHostsRequest.newInstance(hostSetToInclude);
            clientRMService.includeExternalHosts(request);
            ReplaceLabelsOnNodeRequest replaceLabelRequest = ReplaceLabelsOnNodeRequest.newInstance(nodeLabels);
            rmAdminProxy.replaceLabelsOnNode(replaceLabelRequest);
            LOG.info("includeExternalHosts and replace node label request succeed.");
          } catch (Exception e) {
            LOG.error("Failed to include hosts or replace the node label");
          }
          createNMJob(
            instanceNumber,
            clientWrapper.getResourceManager(),
            clientWrapper.getJobSvc(),
            clientWrapper.getPelotonZKInfo());
        }
      } catch (Exception e) {
        LOG.error("Exception occurred", e);
      }
    }
  }

  /**
   * This can go to config file and basic on getFields GRPC functions, the values can be populated in a more
   * automated way
   * @param desiredInstanceCount the desired NM instance count in Peloton shared pool
   */
  protected void createNMJob(
      int desiredInstanceCount,
      ResourceManagerBlockingStub resourceManager,
      JobServiceBlockingStub jobSvc,
      PelotonZKInfo zkInfo) throws IOException {
    LOG.info("createNMJob in Peloton cluster " + zkInfo);
    /**
     * Initialize job spec from Peloton Job spec
     */
    // Start 10% of NM instances in a batch
    int batchSize = (int) (desiredInstanceCount * 0.1);
    if (batchSize < 1) {
      batchSize = 1;
    }
    PelotonJobSpec.updateJobSpecRegion(zkInfo.getRegion());
    PelotonJobSpec.updateJobSpecZone(zkInfo.getZone());
    PelotonJobSpec.updateJobSpecOdinInstance(conf.get(YarnConfiguration.PELOTON_CLUSTER_ID));
    Stateless.JobSpec jobSpec = Stateless.JobSpec.newBuilder().
        setName(PelotonJobSpec.getJobName()).
        setOwningTeam(PelotonJobSpec.getOwningTeam()).
        addLdapGroups(PelotonJobSpec.getLdapGroups()).
        setDescription(PelotonJobSpec.getDescription()).
        addAllLabels(PelotonJobSpec.getLabels()).
        setRespoolId(PelotonJobSpec.getRespoolId(resourceManager, zkInfo.getResourcePoolPath())).
        setHostpool(PelotonJobSpec.getHostPool()).
        setInstanceCount(desiredInstanceCount).
        setDefaultSpec(PelotonJobSpec.getPodSpec(getConf())).
        build();

    Peloton.JobID existingJob = queryNMJob(jobSvc, zkInfo.getZKAddress());
    if (null == existingJob) {
      // Create request
      CreateSpec createSpec = Stateless.CreateSpec.newBuilder().setBatchSize(batchSize).build();
      StatelessSvc.CreateJobRequest request = StatelessSvc.CreateJobRequest.newBuilder().
          setSpec(jobSpec).
          setCreateSpec(createSpec).
          build();
      // Create Job
      LOG.info("Creating Peloton job for the node manager");
      StatelessSvc.CreateJobResponse response = jobSvc.createJob(request);
      // Log response
      LOG.info("Create Job Response - JobId: {}", response.getJobId());
    } else {
      // Update Job
      LOG.info("NM stateless job exists in the Peloton cluster");

      Stateless.JobInfo jobInfo = getJob(existingJob, jobSvc);
      int existingInstanceCount = jobInfo.getSpec().getInstanceCount();
      String runningNMImage = jobInfo.getSpec().getDefaultSpec().getContainers(0).getImage();
      String desiredNMImage = PelotonJobSpec.getNMImage(conf);

      if ((existingInstanceCount == desiredInstanceCount)
        && (runningNMImage.equals(desiredNMImage))) {
        LOG.info("Existing NM job already has the desired instance count {} and NM image {}, no need to refresh",
          desiredInstanceCount, desiredNMImage);
        return;
      }

      if (!runningNMImage.equals(desiredNMImage)) {
        LOG.info("Current running NM image {} is different from conf {}, need to replace NM stateless job",
          runningNMImage, desiredNMImage);
      }
      if (existingInstanceCount != desiredInstanceCount) {
        LOG.info("Current NM instance count {} is different from desired count {}, need to replace NM stateless job",
          existingInstanceCount, desiredInstanceCount);
      }

      LOG.info("Replacing NM stateless job {} on Peloton with desired instance count {} and NM image {}",
          existingJob.getValue(), desiredInstanceCount, desiredNMImage);
      UpdateSpec updateSpec = Stateless.UpdateSpec.newBuilder().setBatchSize(batchSize).build();
      StatelessSvc.ReplaceJobRequest req = StatelessSvc.ReplaceJobRequest.newBuilder().
          setJobId(existingJob).
          setVersion(jobInfo.getStatus().getVersion()).
          setUpdateSpec(updateSpec).
          setSpec(jobSpec).
          build();
      StatelessSvc.ReplaceJobResponse res = jobSvc.replaceJob(req);
      LOG.info("Replace Job Response: {}", res);
    }
  }

  /**
   * Query exising NM jobs on Peloton cluster
   *
   * @return Peloton.JobID
   */
  public Peloton.JobID queryNMJob(JobServiceBlockingStub jobSvc, String pelotonZK) {
    // TODO: this should be improved to persist and query job using job id
    long start = monotonicNow();
    StatelessSvc.QueryJobsRequest req =
        StatelessSvc.QueryJobsRequest.newBuilder().
            setSpec(Stateless.QuerySpec.newBuilder().
                addLabels(Peloton.Label.newBuilder().
                    setKey("YARNService").
                    setValue("NodeManager").
                    build()).
                build()).
            build();
    StatelessSvc.QueryJobsResponse res = jobSvc.queryJobs(req);
    if (res.getRecordsCount() == 1) {
      LOG.info("Query result is successful and NM job does exist in Peloton Cluster, job id: {}",
          res.getRecords(0).getJobId().getValue());
      metrics.queryNMJob(monotonicNow() - start);
      return res.getRecords(0).getJobId();
    } else if (res.getRecordsCount() > 1) {
      LOG.error("Peloton cluster {} has more than one NM job running, please fix this! "
              + "Returning the first NM job {}", pelotonZK,
          res.getRecords(0).getJobId().getValue());
      metrics.incrQueryJobFailure();
      return res.getRecords(0).getJobId();
    }
    LOG.info("Query result is not-successful and NM job does not exist in Peloton Cluster");
    metrics.incrQueryJobFailure();
    return null;
  }

  /**
   * Get Stateless job info
   * @param jobID
   * @return Stateless.JobInfo
   */
  public Stateless.JobInfo getJob(Peloton.JobID jobID, JobServiceBlockingStub jobSvc) {
    StatelessSvc.GetJobRequest req =
        StatelessSvc.GetJobRequest.newBuilder().
            setJobId(jobID).
            build();
    StatelessSvc.GetJobResponse res = jobSvc.getJob(req);
    return res.getJobInfo();
  }

  private List<PelotonZKInfo> getPelotonZkList(String cluster) {
    try {
      GetPelotonZKInfoListByClusterResponse response = getPelotonZKConfRecordStore().getPelotonZKConfListByCluster(
          GetPelotonZKInfoListByClusterRequest.newInstance(cluster)
      );
      return response.getPelotonZKInfoList();
    } catch (IOException e) {
      LOG.error("Failed to get Peloton Zk Configuration from store.", e);
      return null;
    }
  }

  private PelotonZKConfRecordStore getPelotonZKConfRecordStore() throws IOException {
    if (this.pelotonZKConfRecordStore == null) {
      this.pelotonZKConfRecordStore = routerStateStore.getRegisteredRecordStore(
          PelotonZKConfRecordStore.class);
      if (this.pelotonZKConfRecordStore == null) {
        throw new IOException("PelotonZKConfRecordStore is not available.");
      }
    }
    return this.pelotonZKConfRecordStore;
  }

  private PelotonNodeLabelRecordStore getPelotonNodeLabelRecordStore() throws IOException {
    if (this.pelotonNodeLabelRecordStore == null) {
      this.pelotonNodeLabelRecordStore = routerStateStore.getRegisteredRecordStore(
          PelotonNodeLabelRecordStore.class);
      if (this.pelotonNodeLabelRecordStore == null) {
        throw new IOException("PelotonNodeLabelRecordStore is not available.");
      }
    }
    return this.pelotonNodeLabelRecordStore;
  }

  public Configuration getConf() {
    return this.conf;
  }

}
