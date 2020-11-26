package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.api.protocolrecords.GetExternalIncludedHostsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetExternalIncludedHostsRequestPBImpl;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.GetExternalIncludedHostsResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scorer.ScorerService.HostScoreInfo;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class to verify Scorer Service
 */
public class TestScorerService {
  static String HOST1 = "agent1-phx2";
  static String HOST2 = "agent2-phx2";
  static int HOST_PORT = 123;
  static String PREEMPTIBLE_QUEUE = "preemptible-queue";
  static String NON_PREEMPTIBLE_QUEUE = "non-preemptible-queue";

  ScorerService scorerService;
  ClientRMService mockClientRMService;
  ResourceScheduler mockScheduler;
  Dispatcher mockDispatcher;
  EventHandler mockEventHandler;
  QueueInfo mockPreemptibleQueueInfo;
  QueueInfo mockNonPreemptibleQueueInfo;
  RMContext mockRMContext;
  GetExternalIncludedHostsResponse getExternalIncludedHostsResponse;
  ScorerEventDispatcherMetrics metrics = ScorerEventDispatcherMetrics.registerMetrics();

  @Before
  public void setup() throws IOException {
    mockClientRMService = mock(ClientRMService.class);
    getExternalIncludedHostsResponse = new GetExternalIncludedHostsResponsePBImpl();
    when(mockClientRMService.getExternalIncludedHosts(any(GetExternalIncludedHostsRequestPBImpl.class))).thenReturn(getExternalIncludedHostsResponse);

    mockPreemptibleQueueInfo = mock(QueueInfo.class);
    when(mockPreemptibleQueueInfo.getPreemptionDisabled()).thenReturn(false);

    mockNonPreemptibleQueueInfo = mock(QueueInfo.class);
    when(mockNonPreemptibleQueueInfo.getPreemptionDisabled()).thenReturn(true);

    mockScheduler = mock(ResourceScheduler.class);
    when(mockScheduler.getQueueInfo(PREEMPTIBLE_QUEUE, false, false)).thenReturn(
      mockPreemptibleQueueInfo);
    when(mockScheduler.getQueueInfo(NON_PREEMPTIBLE_QUEUE, false, false)).thenReturn(
      mockNonPreemptibleQueueInfo);

    mockRMContext = mock(RMContextImpl.class);
    when(mockRMContext.getClientRMService()).thenReturn(mockClientRMService);
    when(mockRMContext.getScheduler()).thenReturn(mockScheduler);

    scorerService = new ScorerService();
    scorerService.setRMContext(mockRMContext);
    scorerService.setScorerMetrics(metrics);

    mockDispatcher = mock(Dispatcher.class);
    when(mockRMContext.getDispatcher()).thenReturn(mockDispatcher);
    mockEventHandler = mock(EventHandler.class);
    when(mockDispatcher.getEventHandler()).thenReturn(mockEventHandler);
    doNothing().when(mockEventHandler).handle(any(Event.class));
  }

  @Test
  public void testIncludePelotonHostsEmpty() {
    scorerService.updateExcludeHosts(new HashSet<String>());
    assertEquals(0, scorerService.getHostsScoreMap().size());
  }

  @Test
  public void testIncludePelotonHostsNull() {
    scorerService.updateExcludeHosts(null);
    assertEquals(0, scorerService.getHostsScoreMap().size());
  }

  @Test
  public void testIncludePelotonHosts() {
    Set<String> initialHostsSet = new HashSet<>(Arrays. asList("host1", "host2"));
    scorerService.updateIncludeHosts(initialHostsSet);
    //host score map will have host1, 2
    assertEquals(2, scorerService.getHostsScoreMap().size());

    Set<String> updatedHostsSet = new HashSet<>(Arrays. asList("host2", "host3", "host4"));
    scorerService.updateIncludeHosts(updatedHostsSet);
    //host score map will have host1, 3, 4
    assertEquals(3, scorerService.getHostsScoreMap().size());

    Set<String> updatedHostsSet2 = new HashSet<>(Arrays. asList("host5"));
    scorerService.updateIncludeHosts(updatedHostsSet2);
    //host score map will have host5
    assertEquals(1, scorerService.getHostsScoreMap().size());

    Set<String> updatedHostsSet3 = new HashSet<>();
    scorerService.updateIncludeHosts(updatedHostsSet3);
    //host score map will be empty
    assertEquals(0, scorerService.getHostsScoreMap().size());
  }

  private void initHosts() {
    Set<String> hosts = new HashSet<>(Arrays.asList(HOST1, HOST2));
    getExternalIncludedHostsResponse.setIncludedHosts(hosts);
    scorerService.initHostScoreMap();
  }

  @Test
  public void testScorerHostEvent() {
    //add a non-AM container to HOST1
    ConcurrentMap<String, HostScoreInfo> hostsScoreMap = scorerService.getHostsScoreMap();
    hostsScoreMap.clear();
    hostsScoreMap.put(HOST1, new HostScoreInfo(HOST1, 1, 0 ,1));

    //update include hosts with new host3
    ScorerHostEvent event = new ScorerHostEvent(new HashSet<String>(Arrays.asList(HOST1, "host3")),
      ScorerEventType.INCLUDE_HOSTS_UPDATE);
    scorerService.handle(event);
    assertEquals(2, scorerService.getHostsScoreMap().size());

    Map<String, HostScoreInfo> scoreInfoMap = scorerService.getHostsScoreMap();
    HostScoreInfo host1Score = scoreInfoMap.get(HOST1);
    assertEquals(0, host1Score.numAMs);
    assertEquals(1, host1Score.numContainers);
    assertEquals(1, host1Score.numNonPreemptible);
    HostScoreInfo host3Score = scoreInfoMap.get("host3");
    assertEquals(0, host3Score.numAMs);
    assertEquals(0, host3Score.numContainers);
    assertEquals(0, host3Score.numNonPreemptible);

    //exclude host3
    event = new ScorerHostEvent(new HashSet<String>(Arrays.asList("host3")),
      ScorerEventType.EXCLUDE_HOSTS_UPDATE);
    scorerService.handle(event);
    assertEquals(1, scorerService.getHostsScoreMap().size());
    assertEquals(0, host1Score.numAMs);
    assertEquals(1, host1Score.numContainers);
    assertEquals(1, host1Score.numNonPreemptible);
  }

  @Test
  public void testHostScoreSorting() {
    //The score comparator will sort hosts using numNonPreemptible -> nonAMs -> numContainers
    Map<String, HostScoreInfo> scoreInfoMap = scorerService.getHostsScoreMap();
    scoreInfoMap.put("host1", new HostScoreInfo("host1", 3, 3, 3));
    scoreInfoMap.put("host2", new HostScoreInfo("host2", 3, 2, 2));
    scoreInfoMap.put("host3", new HostScoreInfo("host3", 3, 2, 3));

    //The host list after sorting should be : host2, host3, host1
    assertEquals(Arrays.asList("host2", "host3", "host1"), scorerService.getOrderedHostList());
  }

  @Test
  public void testHostScoreSortingNoHosts() {
    Map<String, HostScoreInfo> scoreInfoMap = scorerService.getHostsScoreMap();
    scoreInfoMap.clear();
    assertEquals(new ArrayList<String>(), scorerService.getOrderedHostList());
  }

  @Test
  public void testHostScoreSortingOneHost() {
    Map<String, HostScoreInfo> scoreInfoMap = scorerService.getHostsScoreMap();
    scoreInfoMap.put("host1", new HostScoreInfo("host1", 3, 3, 3));
    assertEquals(Arrays.asList("host1"), scorerService.getOrderedHostList());
  }

  @Test
  public void testUpdateHostScore() {
    initHosts();

    ConcurrentMap<NodeId, RMNode> rmNodes = new ConcurrentHashMap<NodeId, RMNode>();
    when(mockRMContext.getRMNodes()).thenReturn(rmNodes);
    when(mockRMContext.getScheduler()).thenReturn(mockScheduler);

    RMNodeImpl rmNode1 = mock(RMNodeImpl.class);
    ContainerId containerId1 = mock(ContainerId.class);
    when(containerId1.toString()).thenReturn("containerId1");
    ContainerId containerId2 = mock(ContainerId.class);
    when(containerId2.toString()).thenReturn("containerId2");
    when(rmNode1.getLaunchedContainers()).thenReturn(
      new HashSet<ContainerId>(Arrays.asList(containerId1, containerId2)));
    when(rmNode1.getHostName()).thenReturn(HOST1);
    NodeId nodeId1 = NodeId.newInstance(HOST1, HOST_PORT);
    rmNodes.put(nodeId1, rmNode1);

    RMNodeImpl rmNode2 = mock(RMNodeImpl.class);
    ContainerId containerId3 = mock(ContainerId.class);
    when(containerId3.toString()).thenReturn("containerId3");
    ContainerId containerId4 = mock(ContainerId.class);
    when(containerId4.toString()).thenReturn("containerId4");
    when(rmNode2.getLaunchedContainers()).thenReturn(
      new HashSet<ContainerId>(Arrays.asList(containerId3, containerId4)));
    when(rmNode2.getHostName()).thenReturn(HOST2);
    NodeId nodeId2 = NodeId.newInstance(HOST2, HOST_PORT);
    rmNodes.put(nodeId2, rmNode2);

    RMContainerImpl rmContainer1 = mock(RMContainerImpl.class);
    when(rmContainer1.getCreationTime()).thenReturn(System.currentTimeMillis());
    when(rmContainer1.isAMContainer()).thenReturn(true);
    when(rmContainer1.getQueueName()).thenReturn(NON_PREEMPTIBLE_QUEUE);
    when(mockScheduler.getRMContainer(containerId1)).thenReturn(rmContainer1);

    RMContainerImpl rmContainer2 = mock(RMContainerImpl.class);
    when(rmContainer2.getCreationTime()).thenReturn(System.currentTimeMillis());
    when(rmContainer2.isAMContainer()).thenReturn(false);
    when(rmContainer2.getQueueName()).thenReturn(NON_PREEMPTIBLE_QUEUE);
    when(mockScheduler.getRMContainer(containerId2)).thenReturn(rmContainer2);

    RMContainerImpl rmContainer3 = mock(RMContainerImpl.class);
    when(rmContainer3.getCreationTime()).thenReturn(System.currentTimeMillis());
    when(rmContainer3.isAMContainer()).thenReturn(false);
    when(rmContainer3.getQueueName()).thenReturn(PREEMPTIBLE_QUEUE);
    when(mockScheduler.getRMContainer(containerId3)).thenReturn(rmContainer3);

    RMContainerImpl rmContainer4 = mock(RMContainerImpl.class);
    when(rmContainer4.getCreationTime()).thenReturn(System.currentTimeMillis());
    when(rmContainer4.isAMContainer()).thenReturn(false);
    when(rmContainer4.getQueueName()).thenReturn(NON_PREEMPTIBLE_QUEUE);
    when(mockScheduler.getRMContainer(containerId4)).thenReturn(rmContainer4);

    scorerService.updateHostScore();
    assertEquals(2, scorerService.getHostsScoreMap().size());
    assertEquals(2, scorerService.getHostsScoreMap().get(HOST1).numNonPreemptible);
    assertEquals(1, scorerService.getHostsScoreMap().get(HOST1).numAMs);
    assertEquals(2, scorerService.getHostsScoreMap().get(HOST1).numContainers);
    assertEquals(0, scorerService.getHostsScoreMap().get(HOST1).containerRunningTime, 60);

    assertEquals(1, scorerService.getHostsScoreMap().get(HOST2).numNonPreemptible);
    assertEquals(0, scorerService.getHostsScoreMap().get(HOST2).numAMs);
    assertEquals(2, scorerService.getHostsScoreMap().get(HOST2).numContainers);
    assertEquals(0, scorerService.getHostsScoreMap().get(HOST2).containerRunningTime, 60);
  }
}
