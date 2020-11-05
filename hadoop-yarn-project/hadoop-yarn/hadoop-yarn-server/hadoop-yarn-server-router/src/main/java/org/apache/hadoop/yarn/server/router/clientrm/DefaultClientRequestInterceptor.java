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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.metrics.RouterRPCPerformanceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Extends the {@code AbstractRequestInterceptorClient} class and provides an
 * implementation that simply forwards the client requests to the cluster
 * resource manager.
 *
 */
public class DefaultClientRequestInterceptor
    extends AbstractClientRequestInterceptor {
  private ApplicationClientProtocol clientRMProxy;
  private RouterRPCPerformanceMonitor monitor;

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultClientRequestInterceptor.class);

  @Override
  public void init(String userName) {
    super.init(userName);

    final Configuration conf = this.getConf();
    try {
      clientRMProxy =
          user.doAs(new PrivilegedExceptionAction<ApplicationClientProtocol>() {
            @Override
            public ApplicationClientProtocol run() throws Exception {
              return ClientRMProxy.createRMProxy(conf,
                  ApplicationClientProtocol.class);
            }
          });
    } catch (Exception e) {
      throw new YarnRuntimeException(
          "Unable to create the interface to reach the YarnRM", e);
    }
  }

  @Override
  public void setNextInterceptor(ClientRequestInterceptor next) {
    throw new YarnRuntimeException(
        "setNextInterceptor is being called on DefaultRequestInterceptor,"
            + "which should be the last one in the chain "
            + "Check if the interceptor pipeline configuration is correct");
  }

  @Override
  public void setRpcMonitor(RouterRPCPerformanceMonitor monitor) {
    this.monitor = monitor;
  }

  @Override
  public RouterRPCPerformanceMonitor getRpcMonitor() {
    return monitor;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {
    long startTime = monotonicNow();
    monitor.proxyOp();
    GetNewApplicationResponse response = null;
    try {
      response = clientRMProxy.getNewApplication(request);
    } catch (Exception e) {
      LOG.warn("Unable to create a new ApplicationId", e);
    }

    if (response != null) {
      long stopTime = monotonicNow();
      monitor.getRPCMetrics().succeededAppsCreated(stopTime - startTime);
      return response;
    }

    monitor.getRPCMetrics().incrAppsFailedCreated();
    String errMsg = "Fail to create a new application.";
    LOG.error(errMsg);
    throw new YarnException(errMsg);
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    long startTime = monotonicNow();
    monitor.proxyOp();
    ApplicationId applicationId =
        request.getApplicationSubmissionContext().getApplicationId();
    SubmitApplicationResponse response = null;
    try {
      response = clientRMProxy.submitApplication(request);
    } catch (Exception e) {
      LOG.warn("Unable to submit the application " + applicationId, e);
      monitor.proxyOpFailed();
    }

    if (response != null) {
      LOG.info("Application "
          + request.getApplicationSubmissionContext().getApplicationName()
          + " with appId " + applicationId);
      long stopTime = monotonicNow();
      monitor.getRPCMetrics().succeededAppsSubmitted(stopTime - startTime);
      monitor.proxyOpComplete();
      return response;
    }

    monitor.getRPCMetrics().incrAppsFailedSubmitted();
    String errMsg = "Application "
      + request.getApplicationSubmissionContext().getApplicationName()
      + " with appId " + applicationId + " failed to be submitted.";
    LOG.error(errMsg);
    throw new YarnException(errMsg);
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {

    long startTime = monotonicNow();

    if (request == null || request.getApplicationId() == null) {
      monitor.getRPCMetrics().incrAppsFailedKilled();
      RouterServerUtil.logAndThrowException(
          "Missing forceKillApplication request or ApplicationId.", null);
    }
    ApplicationId applicationId = request.getApplicationId();

    KillApplicationResponse response = null;
    try {
      LOG.info("forceKillApplication " + applicationId);
      response = clientRMProxy.forceKillApplication(request);
    } catch (Exception e) {
      monitor.getRPCMetrics().incrAppsFailedKilled();
      LOG.error("Unable to kill the application report for "
          + request.getApplicationId(), e);
      throw e;
    }
    if (response == null) {
      LOG.error("No response when attempting to kill the application "
          + applicationId);
    }
    long stopTime = monotonicNow();
    monitor.getRPCMetrics().succeededAppsKilled(stopTime - startTime);
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    return clientRMProxy.getClusterMetrics(request);
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    return clientRMProxy.getClusterNodes(request);
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    return clientRMProxy.getQueueInfo(request);
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    return clientRMProxy.getQueueUserAcls(request);
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    return clientRMProxy.moveApplicationAcrossQueues(request);
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    return clientRMProxy.getNewReservation(request);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    return clientRMProxy.submitReservation(request);
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    return clientRMProxy.listReservations(request);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    return clientRMProxy.updateReservation(request);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    return clientRMProxy.deleteReservation(request);
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    return clientRMProxy.getNodeToLabels(request);
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    return clientRMProxy.getLabelsToNodes(request);
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    return clientRMProxy.getClusterNodeLabels(request);
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {
    long startTime = monotonicNow();
    monitor.proxyOp();
    if (request == null || request.getApplicationId() == null) {
      monitor.getRPCMetrics().incrAppsFailedRetrieved();
      monitor.proxyOpFailed();
      RouterServerUtil.logAndThrowException(
          "Missing getApplicationReport request or applicationId information.",
          null);
    }

    GetApplicationReportResponse response = null;
    try {
      response = clientRMProxy.getApplicationReport(request);
    } catch (Exception e) {
      monitor.getRPCMetrics().incrAppsFailedRetrieved();
      monitor.proxyOpFailed();
      LOG.error("Unable to get the application report for "
          + request.getApplicationId(), e);
      throw e;
    }
    long stopTime = monotonicNow();
    monitor.getRPCMetrics().succeededAppsRetrieved(stopTime - startTime);
    monitor.proxyOpComplete();
    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of "
          + "the application " + request.getApplicationId());
    }
    return response;
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    long startTime = monotonicNow();
    monitor.proxyOp();
    GetApplicationsResponse response = null;
    try {
      response = clientRMProxy.getApplications(request);
    } catch (Exception e) {
      String errorMsg = "Unable to get applications ";
      LOG.warn(errorMsg, e);
      monitor.getRPCMetrics().incrMultipleAppsFailedRetrieved();
      monitor.proxyOpFailed();
      throw new YarnException(errorMsg);
    }
    long stopTime = monotonicNow();
    monitor.getRPCMetrics().succeededMultipleAppsRetrieved(stopTime - startTime);
    monitor.proxyOpComplete();
    return response;
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    return clientRMProxy.getApplicationAttemptReport(request);
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    return clientRMProxy.getApplicationAttempts(request);
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    return clientRMProxy.getContainerReport(request);
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    return clientRMProxy.getContainers(request);
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    return clientRMProxy.getDelegationToken(request);
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    return clientRMProxy.renewDelegationToken(request);
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    return clientRMProxy.cancelDelegationToken(request);
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    return clientRMProxy.failApplicationAttempt(request);
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {
    return clientRMProxy.updateApplicationPriority(request);
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    return clientRMProxy.signalToContainer(request);
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    return clientRMProxy.updateApplicationTimeouts(request);
  }

  @Override
  public IncludeExternalHostsResponse includeExternalHosts(
          IncludeExternalHostsRequest request)
          throws YarnException, IOException {
    return clientRMProxy.includeExternalHosts(request);
  }

  @Override
  public GetExternalIncludedHostsResponse getExternalIncludedHosts(
          GetExternalIncludedHostsRequest request)
          throws YarnException, IOException {
    return clientRMProxy.getExternalIncludedHosts(request);
  }

  @Override
  public GetOrderedHostsResponse getOrderedHosts(
    GetOrderedHostsRequest request)
    throws YarnException, IOException {
    return clientRMProxy.getOrderedHosts(request);
  }

  @VisibleForTesting
  public void setRMClient(ApplicationClientProtocol clientRM) {
    this.clientRMProxy = clientRM;

  }
}
