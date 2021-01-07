package org.apache.hadoop.yarn.server.router.external.peloton;

import com.peloton.api.v0.host.svc.pb.HostServiceGrpc.HostServiceBlockingStub;
import com.uber.peloton.client.HostManager;
import com.uber.peloton.client.ResourceManager;
import com.uber.peloton.client.StatelessJobService;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonZKInfo;
import peloton.api.v0.respool.ResourceManagerGrpc.ResourceManagerBlockingStub;
import peloton.api.v1alpha.job.stateless.svc.JobServiceGrpc.JobServiceBlockingStub;
import peloton.api.v1alpha.peloton.Peloton.JobID;

public class PelotonClientWrapper {

  private PelotonZKInfo zkInfo;
  private int instanceNumber = 0;

  private HostManager hostManagerClient;
  private ResourceManager resourceManagerClient;
  private StatelessJobService statelessSvcClient;

  private HostServiceBlockingStub hostService;
  private JobServiceBlockingStub jobSvc;
  private ResourceManagerBlockingStub resourceManager;
  private JobID pelotonJobId;

  private PelotonClientWrapper() {}

  public static PelotonClientWrapper newInstance() {
    return new PelotonClientWrapper();
  }

  public void setPelotonZKInfo(PelotonZKInfo zkInfo) {
    this.zkInfo = zkInfo;
  }

  public void setHostManagerClient(HostManager hostManagerClient) {
    this.hostManagerClient = hostManagerClient;
  }

  public void setResourceManagerClient(ResourceManager resourceManagerClient) {
    this.resourceManagerClient = resourceManagerClient;
  }

  public void setStatelessSvcClient(StatelessJobService statelessSvcClient) {
    this.statelessSvcClient = statelessSvcClient;
  }

  public void setHostService(HostServiceBlockingStub hostService) {
    this.hostService = hostService;
  }

  public void setJobSvc(JobServiceBlockingStub jobSvc) {
    this.jobSvc = jobSvc;
  }

  public void setResourceManager(ResourceManagerBlockingStub resourceManager) {
    this.resourceManager = resourceManager;
  }

  public void setPelotonJobId (JobID jobId) {
    this.pelotonJobId = jobId;
  }

  public void setInstanceNumber(int instanceNumber) {
    this.instanceNumber = instanceNumber;
  }

  public PelotonZKInfo getPelotonZKInfo() {
    return zkInfo;
  }

  public HostServiceBlockingStub getHostService() {
    return hostService;
  }

  public JobServiceBlockingStub getJobSvc() {
    return jobSvc;
  }

  public HostManager getHostManagerClient() {
    return hostManagerClient;
  }

  public ResourceManager getResourceManagerClient() {
    return resourceManagerClient;
  }

  public StatelessJobService getStatelessSvcClient() {
    return statelessSvcClient;
  }

  public ResourceManagerBlockingStub getResourceManager() {
    return resourceManager;
  }
  public JobID getPelotonJobId () {
    return pelotonJobId;
  }

  public int getInstanceNumber() {
    return instanceNumber;
  }
}
