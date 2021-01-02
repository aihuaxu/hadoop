package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.QueryResult;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.GetPelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.impl.pb.SavePelotonNodeLabelRequestPBImpl;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonNodeLabel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
public class TestPelotonNodeLabelRecordStore {

  private StateStoreDriver stateStoreDriver;
  private PelotonNodeLabelRecordStore pelotonNodeLabelRecordStore;
  private String TARGET_LABEL = "test-label";

  @Before
  public void setup() throws IOException {
    stateStoreDriver = mock(StateStoreDriver.class);
    List<PelotonNodeLabel> records = new ArrayList<>();
    records.add(PelotonNodeLabel.newInstance(TARGET_LABEL));
    QueryResult<PelotonNodeLabel> queryResult = new QueryResult<>(records, 1);
    when(stateStoreDriver.get(PelotonNodeLabel.class)).thenReturn(queryResult);
    when(stateStoreDriver.removeAll(PelotonNodeLabel.class)).thenReturn(true);
    pelotonNodeLabelRecordStore = new PelotonNodeLabelRecordStore(stateStoreDriver);
  }

  @Test
  public void testGetPelotonNodeLabel() throws Exception {
    GetPelotonNodeLabelRequest request = new GetPelotonNodeLabelRequestPBImpl();
    pelotonNodeLabelRecordStore.loadCache(true);
    GetPelotonNodeLabelResponse response = pelotonNodeLabelRecordStore.getPelotonNodeLabel(request);
    Assert.assertEquals(TARGET_LABEL, response.getPelotonNodeLabel());
  }

  @Test
  public void testSavePelotonNodeLabel() throws Exception {
    SavePelotonNodeLabelRequest request = new SavePelotonNodeLabelRequestPBImpl();
    pelotonNodeLabelRecordStore.loadCache(true);
    SavePelotonNodeLabelResponse response = pelotonNodeLabelRecordStore.savePelotonNodeLabel(request);
    verify(stateStoreDriver, times(1)).removeAll(PelotonNodeLabel.class);
  }

  @Test
  public void testGetRecordName() {
    Assert.assertEquals("PelotonNodeLabel", pelotonNodeLabelRecordStore.getRecordName(PelotonNodeLabel.class));
  }
}
