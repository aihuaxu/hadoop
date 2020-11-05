/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetExternalIncludedHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetExternalIncludedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetOrderedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncludeExternalHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.router.metrics.RouterRPCPerformanceMonitor;
import org.apache.hadoop.yarn.server.router.security.RouterDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.router.security.RouterSecurityManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * RouterClientRMService is a service that runs on each router that can be used
 * to intercept and inspect {@link ApplicationClientProtocol} messages from
 * client to the cluster resource manager. It listens
 * {@link ApplicationClientProtocol} messages from the client and creates a
 * request intercepting pipeline instance for each client. The pipeline is a
 * chain of {@link ClientRequestInterceptor} instances that can inspect and
 * modify the request/response as needed. The main difference with
 * AMRMProxyService is the protocol they implement.
 */
public class RouterClientRMService extends AbstractService
    implements ApplicationClientProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterClientRMService.class);

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private Server server;
  private InetSocketAddress listenerEndpoint;
  /** Router security manager to handle token operations. */
  private RouterSecurityManager securityManager = null;

  // For each user we store an interceptors' pipeline.
  // For performance issue we use LRU cache to keep in memory the newest ones
  // and remove the oldest used ones.
  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  public RouterClientRMService() {
    super(RouterClientRMService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting Router ClientRMService");
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation.setConfiguration(conf);

    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.ROUTER_BIND_HOST,
            YarnConfiguration.ROUTER_CLIENTRM_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_PORT);
    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(
        new LRUCacheHashMap<String, RequestInterceptorChainWrapper>(
            maxCacheSize, true));

    Configuration serverConf = new Configuration(conf);

    int numWorkerThreads =
        serverConf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT);

    // Create security manager monitor
    this.securityManager = new RouterSecurityManager(conf);

    this.server = rpc.getServer(ApplicationClientProtocol.class, this,
        listenerEndpoint, serverConf, securityManager.getSecretManager(), numWorkerThreads);

    // Set service-level authorization security policy
    boolean serviceAuthEnabled = conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
    LOG.info("Router ClientRMService Auth is enabled: " + serviceAuthEnabled);
    if (serviceAuthEnabled) {
      server.refreshServiceAclWithLoadedConfiguration(conf, RMPolicyProvider.getInstance());
    }
    this.server.start();
    LOG.info("Router ClientRMService listening on address: "
        + this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping Router ClientRMService");
    if (this.server != null) {
      this.server.stop();
    }
    userPipelineMap.clear();
    super.serviceStop();
  }

  /**
   * Returns the comma separated intercepter class names from the configuration.
   *
   * @param conf
   * @return the intercepter class names as an instance of ArrayList
   */
  private List<String> getInterceptorClassNames(Configuration conf) {
    String configuredInterceptorClassNames =
        conf.get(YarnConfiguration.ROUTER_CLIENTRM_INTERCEPTOR_CLASS_PIPELINE,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_INTERCEPTOR_CLASS);

    List<String> interceptorClassNames = new ArrayList<String>();
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }

    return interceptorClassNames;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getNewApplication(request);
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().submitApplication(request);
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().forceKillApplication(request);
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getClusterMetrics(request);
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getClusterNodes(request);
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getQueueInfo(request);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getQueueUserAcls(request);
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().moveApplicationAcrossQueues(request);
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getNewReservation(request);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().submitReservation(request);
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().listReservations(request);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().updateReservation(request);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().deleteReservation(request);
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getNodeToLabels(request);
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getLabelsToNodes(request);
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getClusterNodeLabels(request);
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getApplicationReport(request);
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getApplications(request);
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getApplicationAttemptReport(request);
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getApplicationAttempts(request);
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getContainerReport(request);
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getContainers(request);
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException {
    long start = monotonicNow();
    RouterRPCPerformanceMonitor rpcMonitor = null;
    try {
      RequestInterceptorChainWrapper pipeline = getInterceptorChain();
      rpcMonitor = pipeline.getRpcMonitor();
    } catch (IOException e) {
      LOG.info("Cannot get rpcMonitor for getDelegationToken");
    }
    try {
      GetDelegationTokenResponse response =
              recordFactory.newRecordInstance(GetDelegationTokenResponse.class);
      Token<RouterDelegationTokenIdentifier> realRouterDToken = securityManager.getDelegationToken(request.getRenewer());
      response.setRMDelegationToken(
              BuilderUtils.newDelegationToken(
              realRouterDToken.getIdentifier(),
              realRouterDToken.getKind().toString(),
              realRouterDToken.getPassword(),
              realRouterDToken.getService().toString()
      ));
      long end = monotonicNow();
      if (rpcMonitor != null) {
        rpcMonitor.getRPCMetrics().succeededDTRetrieved(end - start);
      }
      return response;
    } catch(IOException io) {
      if (rpcMonitor != null) {
        rpcMonitor.getRPCMetrics().incrDTRetrievedFailure();
      }
      throw RPCUtil.getRemoteException(io);
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException {
    long start = monotonicNow();
    RouterRPCPerformanceMonitor rpcMonitor = null;
    try {
      RequestInterceptorChainWrapper pipeline = getInterceptorChain();
      rpcMonitor = pipeline.getRpcMonitor();
    } catch (IOException e) {
      LOG.info("Cannot get rpcMonitor for renewDelegationToken");
    }
    try {
      org.apache.hadoop.yarn.api.records.Token protoToken = request.getDelegationToken();
      Token<RouterDelegationTokenIdentifier> token = new Token<RouterDelegationTokenIdentifier>(
              protoToken.getIdentifier().array(), protoToken.getPassword().array(),
              new Text(protoToken.getKind()), new Text(protoToken.getService()));
      long nextExpTime = securityManager.renewDelegationToken(token);
      RenewDelegationTokenResponse renewResponse = Records
              .newRecord(RenewDelegationTokenResponse.class);
      renewResponse.setNextExpirationTime(nextExpTime);
      long end = monotonicNow();
      if (rpcMonitor != null) {
        rpcMonitor.getRPCMetrics().succeededDTRenewed(end - start);
      }
      return renewResponse;
    } catch (IOException e) {
      if (rpcMonitor != null) {
        rpcMonitor.getRPCMetrics().incrDTRenewFailure();
      }
      throw RPCUtil.getRemoteException(e);
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException {
    long start = monotonicNow();
    RouterRPCPerformanceMonitor rpcMonitor = null;
    try {
      RequestInterceptorChainWrapper pipeline = getInterceptorChain();
      rpcMonitor = pipeline.getRpcMonitor();
    } catch (IOException e) {
      LOG.info("Cannot get rpcMonitor for renewDelegationToken");
    }
    try {
      org.apache.hadoop.yarn.api.records.Token protoToken = request.getDelegationToken();
      Token<RouterDelegationTokenIdentifier> token = new Token<RouterDelegationTokenIdentifier>(
              protoToken.getIdentifier().array(), protoToken.getPassword().array(),
              new Text(protoToken.getKind()), new Text(protoToken.getService()));
      securityManager.cancelDelegationToken(token);
      long end = monotonicNow();
      if (rpcMonitor != null) {
        rpcMonitor.getRPCMetrics().succeededDTCancelled(end - start);
      }
      return Records.newRecord(CancelDelegationTokenResponse.class);
    } catch (IOException e) {
      if (rpcMonitor != null) {
        rpcMonitor.getRPCMetrics().incrDTCancelFailure();
      }
      throw RPCUtil.getRemoteException(e);
    }
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().failApplicationAttempt(request);
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().updateApplicationPriority(request);
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().signalToContainer(request);
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().updateApplicationTimeouts(request);
  }

  @Override
  public IncludeExternalHostsResponse includeExternalHosts(
          IncludeExternalHostsRequest request)
          throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().includeExternalHosts(request);
  }

  @Override
  public GetExternalIncludedHostsResponse getExternalIncludedHosts(
          GetExternalIncludedHostsRequest request)
          throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getExternalIncludedHosts(request);
  }

  @Override
  public GetOrderedHostsResponse getOrderedHosts(
    GetOrderedHostsRequest request)
    throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    checkInterceptor(pipeline);
    return pipeline.getRootInterceptor().getOrderedHosts(request);
  }

  private RequestInterceptorChainWrapper getInterceptorChain()
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    if (!userPipelineMap.containsKey(user)) {
      initializePipeline(user);
    }
    return userPipelineMap.get(user);
  }

  /**
   * Gets the Request intercepter chains for all the users.
   *
   * @return the request intercepter chains.
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * This method creates and returns reference of the first intercepter in the
   * chain of request intercepter instances.
   *
   * @return the reference of the first intercepter in the chain
   */
  @VisibleForTesting
  protected ClientRequestInterceptor createRequestInterceptorChain() {
    Configuration conf = getConfig();

    List<String> interceptorClassNames = getInterceptorClassNames(conf);

    ClientRequestInterceptor pipeline = null;
    ClientRequestInterceptor current = null;
    RouterRPCPerformanceMonitor rpcMonitor = new RouterRPCPerformanceMonitor();
    rpcMonitor.init();
    for (String interceptorClassName : interceptorClassNames) {
      try {
        Class<?> interceptorClass = conf.getClassByName(interceptorClassName);
        if (ClientRequestInterceptor.class.isAssignableFrom(interceptorClass)) {
          ClientRequestInterceptor interceptorInstance =
              (ClientRequestInterceptor) ReflectionUtils
                  .newInstance(interceptorClass, conf);
          interceptorInstance.setRpcMonitor(rpcMonitor);
          if (pipeline == null) {
            pipeline = interceptorInstance;
            current = interceptorInstance;
            continue;
          } else {
            current.setNextInterceptor(interceptorInstance);
            current = interceptorInstance;
          }
        } else {
          throw new YarnRuntimeException(
              "Class: " + interceptorClassName + " not instance of "
                  + ClientRequestInterceptor.class.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException(
            "Could not instantiate ApplicationClientRequestInterceptor: "
                + interceptorClassName,
            e);
      }
    }

    if (pipeline == null) {
      throw new YarnRuntimeException(
          "RequestInterceptor pipeline is not configured in the system");
    }
    return pipeline;
  }

  /**
   * Allow access to the client RPC server for testing.
   *
   * @return The RPC server.
   */
  @VisibleForTesting
  public Server getServer() {
    return server;
  }

  /**
   * Initializes the request intercepter pipeline for the specified application.
   *
   * @param user
   */
  private void initializePipeline(String user) {
    RequestInterceptorChainWrapper chainWrapper = null;
    synchronized (this.userPipelineMap) {
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return;
      }

      chainWrapper = new RequestInterceptorChainWrapper();
      this.userPipelineMap.put(user, chainWrapper);
    }

    // We register the pipeline instance in the map first and then initialize it
    // later because chain initialization can be expensive and we would like to
    // release the lock as soon as possible to prevent other applications from
    // blocking when one application's chain is initializing
    LOG.info("Initializing request processing pipeline for application "
        + "for the user: {}", user);

    try {
      ClientRequestInterceptor interceptorChain =
          this.createRequestInterceptorChain();
      interceptorChain.init(user);
      chainWrapper.init(interceptorChain);
    } catch (Exception e) {
      synchronized (this.userPipelineMap) {
        this.userPipelineMap.remove(user);
      }
      throw e;
    }
  }

  private void checkInterceptor(RequestInterceptorChainWrapper pipeline) throws YarnException {
    if (pipeline == null || pipeline.getRootInterceptor() == null) {
      LOG.warn("Interceptor not initialized yet.");
      throw new YarnException("Client RM not initialized yet.");
    }
    pipeline.getRpcMonitor().startOp();
    pipeline.getRpcMonitor().getRPCMetrics().setCurrentDTCount(securityManager.getCurrentDelegationTokenCount());
  }

  /**
   * Private structure for encapsulating RequestInterceptor and user instances.
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private ClientRequestInterceptor rootInterceptor;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the first interceptor in the pipeline
     */
    public synchronized void init(ClientRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * Gets the root request intercepter.
     *
     * @return the root request intercepter
     */
    public synchronized ClientRequestInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    public synchronized RouterRPCPerformanceMonitor getRpcMonitor() {
      return rootInterceptor.getRpcMonitor();
    }
    /**
     * Shutdown the chain of interceptors when the object is destroyed.
     */
    @Override
    protected void finalize() {
      rootInterceptor.shutdown();
    }
  }
}
