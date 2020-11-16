package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.store.CachedRecordStore;
import org.apache.hadoop.store.driver.StateStoreDriver;
import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.records.PelotonNodeLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PelotonNodeLabelRecordStore extends CachedRecordStore<PelotonNodeLabel> implements PelotonNodeLabelManager {

  public PelotonNodeLabelRecordStore(StateStoreDriver driver) {
    super(PelotonNodeLabel.class, driver);
  }
  private final static Logger LOG =
      LoggerFactory.getLogger(PelotonNodeLabelRecordStore.class);

  @Override
  public String getRecordName(Class<? extends BaseRecord> cls) {
    return RouterServerUtil.getRecordName(cls);
  }

  @Override
  public GetPelotonNodeLabelResponse getPelotonNodeLabel(GetPelotonNodeLabelRequest request) throws IOException {
    GetPelotonNodeLabelResponse response = GetPelotonNodeLabelResponse.newInstance();
    String label = getPelotonNodeLabelFromStore();
    if (label != null) {
      response.setPelotonNodeLabel(label);
    }
    return response;
  }

  @Override
  public SavePelotonNodeLabelResponse savePelotonNodeLabel(SavePelotonNodeLabelRequest request) throws IOException {
    String nodeLabel = request.getNodeLabel();
    getDriver().removeAll(PelotonNodeLabel.class);
    PelotonNodeLabel nodeLabelRecord = PelotonNodeLabel.newInstance(nodeLabel);
    boolean status = getDriver().put(nodeLabelRecord, true, false);
    SavePelotonNodeLabelResponse response = SavePelotonNodeLabelResponse.newInstance(status);
    if (status) {
      loadCache(true);
    }
    return response;
  }

  private String getPelotonNodeLabelFromStore() throws IOException {
    List<PelotonNodeLabel> nodeLabels = getCachedRecords();
    if (nodeLabels == null || nodeLabels.isEmpty()) {
      return null;
    }
    return nodeLabels.get(0).getNodeLabel();
  }
}
