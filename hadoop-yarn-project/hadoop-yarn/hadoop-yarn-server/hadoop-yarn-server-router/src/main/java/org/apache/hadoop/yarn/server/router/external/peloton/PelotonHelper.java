package org.apache.hadoop.yarn.server.router.external.peloton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.peloton.api.v0.host.svc.pb.HostSvc;
import com.peloton.api.v0.host.svc.pb.HostSvc.SetReclaimHostOrderRequest;
import com.peloton.api.v0.host.svc.pb.HostServiceGrpc.HostServiceBlockingStub;
import com.peloton.api.v0.host.svc.pb.HostSvc.ListHostPoolsRequest;
import com.peloton.api.v0.host.svc.pb.HostSvc.ListHostPoolsResponse;
import com.uber.peloton.client.HostManager;
import com.uber.peloton.client.ResourceManager;
import com.uber.peloton.client.StatelessJobService;
import com.uber.peloton.client.shaded.com.google.protobuf.ProtocolStringList;
import java.io.FileInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.external.peloton.PelotonJobSpec.Constants;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peloton.api.v0.host.Host.HostPoolInfo;
import peloton.api.v0.respool.ResourceManagerGrpc.ResourceManagerBlockingStub;
import peloton.api.v1alpha.job.stateless.Stateless;
import peloton.api.v1alpha.job.stateless.svc.JobServiceGrpc.JobServiceBlockingStub;
import peloton.api.v1alpha.job.stateless.svc.StatelessSvc;
import peloton.api.v1alpha.peloton.Peloton;
import peloton.api.v1alpha.job.stateless.Stateless.CreateSpec;
import peloton.api.v1alpha.job.stateless.Stateless.UpdateSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class provides functions to call Peloton APIs for get hosts, create jobs, etc
 */
public class PelotonHelper {

  private final static Logger LOG =
      LoggerFactory.getLogger(PelotonHelper.class);

  private RouterStateStoreService routerStateStore;
  private PelotonZKConfRecordStore pelotonZKConfRecordStore;
  private RouterClientRMService clientRMService;
  private static String CLIENT_NAME = "Yarn Router";
  private List<PelotonClientWrapper> pelotonClientWrapperList;
  private Configuration conf;

  //username and password for Peloton gRPC connection
  private String yarnUser = "";
  private static String yarnPassword = "";

  private Peloton.JobID pelotonJobId;
  private boolean isInitialized = false;

  public PelotonHelper(RouterStateStoreService routerStateStore, RouterClientRMService clientRMService){
    this.routerStateStore = routerStateStore;
    this.clientRMService = clientRMService;
  }

  protected void getOrderedHosts() {
    if (!isInitialized) {
      LOG.warn("not initialized.");
      return;
    }
    GetOrderedHostsRequest request = GetOrderedHostsRequest.newInstance();
    List<String> orderedList = null;
    try {
      //TODO: getOrderedHosts needs to be improved to get ordered hosts from each Peloton cluster separately
      GetOrderedHostsResponse response = clientRMService.getOrderedHosts(request);
      orderedList = response.getOrderedHosts();
    } catch (Exception e) {
      LOG.error("Error getting OrderedHosts", e);
    }
    if (orderedList != null && !orderedList.isEmpty()) {
      for (PelotonClientWrapper clientWrapper : pelotonClientWrapperList) {
        try {
          HostServiceBlockingStub hostService = clientWrapper.getHostService();
          ListHostPoolsResponse pools = listHostsPools(hostService);
          for (HostPoolInfo poolInfo : pools.getPoolsList()) {
            if (poolInfo.getName()
              .equals(PelotonJobSpec.Constants.PELOTON_HOST_POOL_SHARED_TO_YARN)) {
              SetReclaimHostOrderRequest claimerRequest = SetReclaimHostOrderRequest.newBuilder()
                .setHostpool(poolInfo.getName())
                .addAllHosts(orderedList).build();
              hostService.setReclaimHostOrder(claimerRequest);
              LOG.info("setReclaimHostOrder call succeeded for Peloton cluster "
                + clientWrapper.getPelotonZKInfo());
            }
          }
        } catch(Exception e){
          LOG.error("Failed to setReclaimHostOrder for Peloton cluster "
            + clientWrapper.getPelotonZKInfo(), e);
        }
      }
    }
  }

  protected boolean getYarnCredentialOnPeloton() {
    String yopCredentialPath = conf.get(YarnConfiguration.ROUTER_YARN_USER_ON_PELOTON_PATH,
      YarnConfiguration.DEFAULT_ROUTER_YARN_USER_ON_PELOTON_PATH);
    try {
      FileInputStream file = new FileInputStream(yopCredentialPath);
      ObjectMapper om = new ObjectMapper(new YAMLFactory());
      YarnCredentialOnPeloton yopCredential = om.readValue(file, YarnCredentialOnPeloton.class);
      yarnUser = yopCredential.getUsername();
      yarnPassword = yopCredential.getPassword();
      file.close();
    } catch (Exception e) {
      LOG.error("Failed to load Yarn credential on Peloton from Langley", e);
      return false;
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

    // Call Peloton client API to connect its gRPC service,
    // Client API blockingConnect will take care of gRPC&zk timeout or failover.
    // Router can directly call this API without worrying about the connection issues.
    for (PelotonClientWrapper clientWrapper: pelotonClientWrapperList) {
      clientWrapper.setHostService(clientWrapper.getHostManagerClient().blockingConnect());
      clientWrapper.setJobSvc(clientWrapper.getStatelessSvcClient().blockingConnect());
      clientWrapper.setResourceManager(clientWrapper.getResourceManagerClient().blockingConnect());
      LOG.info("Connected to Peloton gRPC services successfully: " + clientWrapper.getPelotonZKInfo());
    }
  }

  protected ListHostPoolsResponse listHostsPools(HostServiceBlockingStub hostService) {
    ListHostPoolsRequest request = ListHostPoolsRequest.newBuilder().build();
    ListHostPoolsResponse response = hostService.listHostPools(request);
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
        ListHostPoolsResponse pools = listHostsPools(clientWrapper.getHostService());
        Set<String> hostSetToInclude = new HashSet<>();
        for (HostPoolInfo poolInfo : pools.getPoolsList()) {
          if (poolInfo.getName().equals(PelotonJobSpec.Constants.PELOTON_HOST_POOL_SHARED_TO_YARN)) {
            instanceNumber = poolInfo.getHostsCount();
            ProtocolStringList hostList = poolInfo.getHostsList();
            hostSetToInclude.addAll(hostList);
            try {
              IncludeExternalHostsRequest request = IncludeExternalHostsRequest.newInstance(hostSetToInclude);
              clientRMService.includeExternalHosts(request);
              LOG.info("includeExternalHosts request succeed.");
            } catch (Exception e) {
              LOG.error("Failed to include hosts");
            }
            LOG.info("Found {} hosts in pool {} for YARN", instanceNumber,
                PelotonJobSpec.Constants.PELOTON_HOST_POOL_SHARED_TO_YARN);
            createNMJob(
                instanceNumber,
                clientWrapper.getResourceManager(),
                clientWrapper.getJobSvc(),
                clientWrapper.getPelotonZKInfo());
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Exception occurred", e);
      }
    }

  }

  /**
   * This can go to config file and basic on getFields GRPC functions, the values can be populated in a more
   * automated way
   * @param instanceCount
   */
  protected void createNMJob(
      int instanceCount,
      ResourceManagerBlockingStub resourceManager,
      JobServiceBlockingStub jobSvc,
      PelotonZKInfo zkInfo) throws IOException {
    LOG.info("createNMJob in Peloton cluster " + zkInfo);
    /**
     * Initialize job spec from Peloton Job spec
     */
    // Start 10% of NM instances in a batch
    int batchSize = (int) (instanceCount * 0.1);
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
        setRespoolId(PelotonJobSpec.getRespoolId(resourceManager)).
        setHostpool(PelotonJobSpec.getHostPool()).
        setInstanceCount(instanceCount).
        setSla(PelotonJobSpec.getSlaSpec()).
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
      LOG.info("Job exists in the Peloton cluster");

      // TODO: queryNMJob response also has an instance count, need to check with Peloton team
      // whether it is desired count or running count
      // if it is desired, then no need to call getJob API
      Stateless.JobInfo jobInfo = getJob(existingJob, jobSvc);
      int existingInstanceCount = jobInfo.getSpec().getInstanceCount();
      if (existingInstanceCount == instanceCount) {
        LOG.info("Existing NM job already has the desired instance count {}, no need to refresh", instanceCount);
        return;
      }

      LOG.info("Updating Peloton job {} for the node manager, current instance count: {}, instance count in shared pool: {}",
          existingJob.getValue(), existingInstanceCount, instanceCount);
      UpdateSpec updateSpec = Stateless.UpdateSpec.newBuilder().setBatchSize(batchSize).build();
      StatelessSvc.ReplaceJobRequest req = StatelessSvc.ReplaceJobRequest.newBuilder().
          setJobId(existingJob).
          setVersion(jobInfo.getStatus().getVersion()).
          setUpdateSpec(updateSpec).
          setSpec(jobSpec).
          build();
      // ToDo - Replace or Refresh ?
      // How to update just hostInstance ? Better way ??
      // Replace Job
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
      return res.getRecords(0).getJobId();
    } else if (res.getRecordsCount() > 1) {
      LOG.error("Peloton cluster {} has more than one NM job running, please fix this! "
              + "Returning the first NM job {}", pelotonZK,
          res.getRecords(0).getJobId().getValue());
      return res.getRecords(0).getJobId();
    }
    LOG.info("Query result is not-successful and NM job does not exist in Peloton Cluster");
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

  public Configuration getConf() {
    return this.conf;
  }

}
