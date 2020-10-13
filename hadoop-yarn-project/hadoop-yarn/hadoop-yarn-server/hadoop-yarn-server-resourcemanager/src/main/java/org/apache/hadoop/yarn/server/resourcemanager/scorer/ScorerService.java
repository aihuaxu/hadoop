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

package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.GetExternalIncludedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetExternalIncludedHostsRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScorerService will sort Peloton hosts based on host score.
 * The following four factors are considered to calculate host score:
 * - number of non-preemptible containers
 * - number of AM containers
 * - number of containers
 * - container running time
 *
 * This is how ScorerService works:
 * - Registers itself to RM's active services so only it will be only running on active RM instance
 * - Registers ScorerEventDispatcher in RM to receive ScorerEvent from NodeListManager and RMContainerImpl
 * - Receives latest include and exclude hosts from NodeListManager
 * - Receives container add / recover / finish / set AM events from RMContainerImpl
 * - Includes a timer task to periodically calculate container running time on these hosts
 * - Uses ScorerEventDispatcherMetrics to emit JMX metrics
 */
@Private
public class ScorerService extends AbstractService implements EventHandler<ScorerEvent> {
  protected static final Logger LOG = LoggerFactory.getLogger(ScorerService.class);
  private static final int SCORER_TIME_PERIOD = 60000;  //1 min

  private static Comparator<Entry<String, HostScoreInfo>> SCORE_COMPARATOR =
    new Comparator<Entry<String, HostScoreInfo>>() {
      public int compare(Entry<String, HostScoreInfo> a, Entry<String, HostScoreInfo> b) {
        if (a.getValue().numNonPreemptible != b.getValue().numNonPreemptible)
          return a.getValue().numNonPreemptible - b.getValue().numNonPreemptible;
        if (a.getValue().numAMs != b.getValue().numAMs)
          return a.getValue().numAMs - b.getValue().numAMs;
        if (a.getValue().numContainers != b.getValue().numContainers)
          return a.getValue().numContainers - b.getValue().numContainers;
        return a.getValue().containerRunningTime - b.getValue().containerRunningTime;
      }
    };

  protected ConcurrentMap<String, HostScoreInfo> hostsScoreMap =
      new ConcurrentHashMap<String, HostScoreInfo>();
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  protected final ReadLock readLock;
  protected final WriteLock writeLock;
  private RMContext rmContext = null;

  private volatile boolean stopped = false;
  private Timer scoreUpdateTimer;
  private ScoreUpdateTimerTask scoreUpdateTimerTask;
  private ScorerEventDispatcherMetrics scorerMetrics;

  /**
   * Information used by host score sorting
   */
  protected static class HostScoreInfo {
    protected String hostName;
    protected int numNonPreemptible = 0;  //number of non-preemptible containers on this host
    protected int numAMs = 0;             //number of application master (AM) containers on this host
    protected int numContainers = 0;      //number of containers on this host including AMs and non-AMs
    protected int containerRunningTime;   //total running time of all live containers

    protected HostScoreInfo(String hostName) {
      this.hostName = hostName;
    }

    protected HostScoreInfo(String hostName, int numNonPreemptible, int numAMs, int numContainers) {
      this.hostName = hostName;
      this.numNonPreemptible = numNonPreemptible;
      this.numAMs = numAMs;
      this.numContainers = numContainers;
    }

    @Override
    public String toString() {
      return "HostScoreInfo{" +
        "hostName='" + hostName + '\'' +
        ", numNonPreemptible=" + numNonPreemptible +
        ", numAMs=" + numAMs +
        ", numContainers=" + numContainers +
        ", containerRunningTime=" + containerRunningTime +
        '}';
    }
  }

  public ScorerService() {
    super(ScorerService.class.getName());
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    scoreUpdateTimer = new Timer(true);
  }

  /**
   * Refresh include hosts in Scorer Service when RM gets the latest include hosts
   * @param includeHosts the list of include hosts
   */
  protected void updateIncludeHosts(Set<String> includeHosts) {
    if (includeHosts == null) {
      LOG.info("Received include hosts is null");
      return;
    }
    LOG.debug("Received {} include hosts : {}", includeHosts.size(), includeHosts);

    try {
      writeLock.lock();
      for (String newHost : includeHosts) {
        if (hostsScoreMap.get(newHost) == null) {
          hostsScoreMap.put(newHost, new HostScoreInfo(newHost));
        }
      }

      //remove a host from host score map if it is not existing anymore
      Set<String> oldHosts = hostsScoreMap.keySet();
      for (String oldHost : oldHosts) {
        if (!includeHosts.contains(oldHost)) {
          hostsScoreMap.remove(oldHost);
        }
      }

      LOG.debug("hostsScoreMap has {} hosts after update include host: {}", hostsScoreMap.size(), hostsScoreMap.keySet());
    } catch (Exception e) {
      LOG.error("Failed to update include hosts, exception", e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Exclude hosts from Scorer Service when they are excluded from RM
   * @param excludeHosts
   */
  protected void updateExcludeHosts(Set<String> excludeHosts) {
    if (excludeHosts == null) {
      LOG.info("Received exclude hosts is null");
      return;
    }
    LOG.info("Received {} exclude hosts : {}", excludeHosts.size(), excludeHosts);

    try {
      writeLock.lock();
      for (String excludeHost : excludeHosts) {
        hostsScoreMap.remove(excludeHost);
      }
    } catch (Exception e) {
      LOG.error("Failed to remove exclude hosts, exception", e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Initialize host score map by getting current include external hosts
   */
  protected void initHostScoreMap() {
    Set<String> hosts = new HashSet<>();
    GetExternalIncludedHostsRequestPBImpl request = new GetExternalIncludedHostsRequestPBImpl();
    request.setTag("Peloton");
    GetExternalIncludedHostsResponse response = rmContext.getClientRMService().getExternalIncludedHosts(request);
    if ((response != null) && (response.getIncludedHosts() != null)) {
      hosts = response.getIncludedHosts();
    }

    //add hosts to host score map
    hostsScoreMap.clear();
    for (String hostName : hosts) {
      hostsScoreMap.put(hostName, new HostScoreInfo(hostName));
    }
    LOG.info("Initialized hostScoreMap with {} hosts: {}", hostsScoreMap.size(), hostsScoreMap.keySet());
  }

  @Override
  protected void serviceStart() throws Exception {
    initHostScoreMap();

    //start a timer task to periodically update container running time in host score
    scoreUpdateTimerTask = new ScoreUpdateTimerTask();
    scoreUpdateTimer.schedule(scoreUpdateTimerTask, 0, SCORER_TIME_PERIOD);
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    if (scoreUpdateTimer != null) {
      scoreUpdateTimer.cancel();
    }
  }

  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  /**
   * Get a list of hosts ordered by score
   *
   * @return a list of hosts
   */
  public List<String> getOrderedHostList() {
    LOG.debug("getOrderedHostList: start sorting");
    long startTime = System.nanoTime();
    List<String> hostsList = new ArrayList<>();
    try {
      readLock.lock();
      // sort hostsScoreMap
      List<Entry<String, HostScoreInfo>> hostsScoreList = new ArrayList<>(
        hostsScoreMap.entrySet());
      Collections.sort(hostsScoreList, SCORE_COMPARATOR);
      for (Entry<String,HostScoreInfo> entry : hostsScoreList) {
        hostsList.add(entry.getKey());
      }
      LOG.debug("getOrderedHostsList: host score list: {}", hostsScoreList);
    } catch (Exception e) {
      LOG.error("Failed to getOrderedHostList, exception: ", e);
    } finally {
      readLock.unlock();
    }
    long processTimeUs = (System.nanoTime() - startTime) / 1000;
    scorerMetrics.setGetOrderedHostsListTimeUs(processTimeUs);
    LOG.info("getOrderedHostList took {} us", processTimeUs);

    return hostsList;
  }

  /**
   * Handle ScorerEvent
   * @param event
   */
  @Override
  public void handle(ScorerEvent event) {
    RMContainer container;
    switch (event.getType()) {
      case CONTAINER_ADDED:
      case CONTAINER_RECOVERED:
      case AM_CONTAINER_ADDED:
      case CONTAINER_FINISHED:
        container = ((ScorerContainerEvent) event).getContainer();
        LOG.info("Received Event {} from container {} on host {}", event.getType(),
          container.getContainerId(),
          container.getNodeId().getHost());
        break;
      case INCLUDE_HOSTS_UPDATE:
      case EXCLUDE_HOSTS_UPDATE:
        Set<String> hosts = ((ScorerHostEvent) event).getHosts();
        LOG.info("Received Event {} with {} hosts", event.getType(), hosts.size());
        break;
      default:
        break;
    }

    switch (event.getType()) {
      case CONTAINER_ADDED:
      case CONTAINER_RECOVERED:
        addContainerToHost(((ScorerContainerEvent) event).getContainer(), false);
        break;
      case AM_CONTAINER_ADDED:
        addContainerToHost(((ScorerContainerEvent) event).getContainer(), true);
        break;
      case CONTAINER_FINISHED:
        removeContainerFromHost(((ScorerContainerEvent) event).getContainer());
        break;
      case AM_CONTAINER_FINISHED:
        removeAMContainerFromHost(((ScorerContainerEvent) event).getContainer());
        break;
      case INCLUDE_HOSTS_UPDATE:
        updateIncludeHosts(((ScorerHostEvent) event).getHosts());
        break;
      case EXCLUDE_HOSTS_UPDATE:
        updateExcludeHosts(((ScorerHostEvent) event).getHosts());
        break;
      default:
        break;
    }
  }

  /**
   * When a container is allocated or set as AM container on a host, increase this host score
   * @param container the allocated container
   * @param isAMContainer whether this container is an AM or not
   */
  private void addContainerToHost(RMContainer container, boolean isAMContainer) {
    //Add this container to HostScoreInfo
    String hostName = container.getNodeId().getHost();
    HostScoreInfo hostScoreInfo = hostsScoreMap.get(hostName);
    if (hostScoreInfo == null) {
      //This container is not from a Peloton host
      return;
    }

    //lock this object in case more than one containers added/finished on this host from different threads
    synchronized (hostScoreInfo) {
      if (isAMContainer) {
        hostScoreInfo.numAMs++;
        scorerMetrics.incrAmContainerAddedCountFromPeloton();
      } else {
        hostScoreInfo.numContainers++;
        scorerMetrics.incrContainerAddedCountFromPeloton();
        try {
          if (rmContext.getScheduler().getQueueInfo(container.getQueueName(), false, false)
            .getPreemptionDisabled()) {
            hostScoreInfo.numNonPreemptible++;
          }
        } catch (IOException e) {
          LOG.warn("Failed to check if container's queue is preemptible, container: {}, queue: {}",
            container.getContainerId(), container.getQueueName(), e);
        }
      }
    }
    LOG.debug("addContainerToHost completed, hostScoreInfo: {}", hostScoreInfo);
  }

  /**
   * After an AM container is finished on a host, decrease this host's score
   * @param container
   */
  private void removeAMContainerFromHost(RMContainer container) {
    HostScoreInfo hostScoreInfo = hostsScoreMap.get(container.getNodeId().getHost());
    if (hostScoreInfo == null) {
      return;
    }

    scorerMetrics.incrAmContainerFinishedCountFromPeloton();
    //lock this object in case more than one containers added to this host from different threads
    synchronized (hostScoreInfo) {
      hostScoreInfo.numAMs--;
    }
    LOG.debug("removeAMContainerFromHost completed, hostScoreInfo: {}", hostScoreInfo);
  }

  /**
   * After a container finishes on a host, decrease this host's score
   */
  private void removeContainerFromHost(RMContainer container) {
    //Remove this container from HostScoreInfo
    HostScoreInfo hostScoreInfo = hostsScoreMap.get(container.getNodeId().getHost());
    if (hostScoreInfo == null) {
      return;
    }

    scorerMetrics.incrContainerFinishedCountFromPeloton();
    //lock this object in case more than one containers added to this host from different threads
    synchronized (hostScoreInfo) {
      if (container.isAMContainer()) {
        rmContext.getDispatcher().getEventHandler().handle(
          new ScorerContainerEvent(container, ScorerEventType.AM_CONTAINER_FINISHED));
      }
      hostScoreInfo.numContainers--;
      try {
        if (rmContext.getScheduler().getQueueInfo(container.getQueueName(), false, false)
          .getPreemptionDisabled()) {
          hostScoreInfo.numNonPreemptible--;
        }
      } catch (IOException e) {
        LOG.warn("Failed to check if container's queue is preemptible, container: {}, queue: {}",
          container.getContainerId(), container.getQueueName(), e);
      }
      //in case container count become negative because of any racing condition
      if ((hostScoreInfo.numAMs < 0) || (hostScoreInfo.numContainers < 0)
        || (hostScoreInfo.numNonPreemptible < 0)) {
        //This should not happen, log this error in case there are any corner cases neglected
        LOG.error("Scorer service container count became negative, hostScoreInfo: {}", hostScoreInfo);
      }
    }
    LOG.debug("removeContainerFromHost completed, hostScoreInfo: {}", hostScoreInfo);
  }

  /**
   * Update container running time in host score
   */
  protected void updateContainerRunningTime() {
    writeLock.lock();
    try {
      //update host container running time
      for (RMNode rmNode : rmContext.getRMNodes().values()) {
        String nodeHostName = rmNode.getHostName();
        HostScoreInfo hostScoreInfo = hostsScoreMap.get(nodeHostName);
        if (hostScoreInfo == null) {
          //this is not a peloton host
          continue;
        }
        hostScoreInfo.containerRunningTime = 0;
        RMNodeImpl rmNodeImpl = (RMNodeImpl) rmNode;
        for (ContainerId containerId : rmNodeImpl.getLaunchedContainers()) {
          RMContainer rmContainer = rmContext.getScheduler().getRMContainer(containerId);
          if (rmContainer == null) {
            continue;
          }
          hostScoreInfo.containerRunningTime +=
            System.currentTimeMillis() - rmContainer.getCreationTime();
        }
      }
    } catch (Exception e) {
      LOG.error("ScoreUpdateTimerTask: host score update failed, exception", e);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Set Scorer metrics
   */
  public void setScorerMetrics(ScorerEventDispatcherMetrics metrics) {
    this.scorerMetrics = metrics;
  }

  /**
   * TimerTask to update container running time in host score
   */
  private class ScoreUpdateTimerTask extends TimerTask {

    @Override
    public void run() {
      LOG.debug("ScoreUpdateTimerTask: Start updating host score...");
      long startTime = System.nanoTime();
      updateContainerRunningTime();
      long processTimeUs = (System.nanoTime() - startTime) / 1000;
      scorerMetrics.setUpdateRunningContainerTimeUs(processTimeUs);
      LOG.info("ScoreUpdateTimerTask took {} us", processTimeUs);
    }
  }

  @VisibleForTesting
  ConcurrentMap<String, HostScoreInfo> getHostsScoreMap() {
    return hostsScoreMap;
  }
}
