package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.minidev.json.JSONArray;
import org.apache.hadoop.yarn.server.resourcemanager.scorer.ScorerService.HostScoreInfo;
import org.codehaus.jackson.JsonFactory;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

public class TestRMScorerInfo {


  @Test
  public void testNoNMsInScorer() {
    ScorerService scorerService = new ScorerService();
    String nmScoreInfo = new RMScorerInfo(scorerService).getNodeManagerScores();
    assertEquals("[]", nmScoreInfo);
  }

  @Test
  public void testNMScores() {
    ScorerService scorerService = new ScorerService();
    ConcurrentMap<String, HostScoreInfo> scoreInfoMap = scorerService.getHostsScoreMap();
    scoreInfoMap.put("host1", new HostScoreInfo("host1", 3, 2, 3, 1000));
    scoreInfoMap.put("host2", new HostScoreInfo("host2", 2, 1, 3, 1500));
    scoreInfoMap.put("host3", new HostScoreInfo("host3", 0, 2, 4, 3000));

    String nmScoreInfo = new RMScorerInfo(scorerService).getNodeManagerScores();
    JsonArray nmScoreArray = new JsonParser().parse(nmScoreInfo).getAsJsonArray();
    assertEquals(3, nmScoreArray.size());

    ConcurrentMap<String, HostScoreInfo> mapResult = new ConcurrentHashMap<String, HostScoreInfo>();
    for (int i = 0; i < 3; i++) {
      JsonObject hostScore = nmScoreArray.get(i).getAsJsonObject();
      mapResult.put(hostScore.get("HostName").getAsString(),
        new HostScoreInfo(hostScore.get("HostName").getAsString(),
          hostScore.get("numNonPreemptible").getAsInt(),
          hostScore.get("numAMs").getAsInt(),
          hostScore.get("numContainers").getAsInt(),
          hostScore.get("containerRunningTime").getAsInt()));
    }
    assertEquals(scoreInfoMap, mapResult);
  }
}
