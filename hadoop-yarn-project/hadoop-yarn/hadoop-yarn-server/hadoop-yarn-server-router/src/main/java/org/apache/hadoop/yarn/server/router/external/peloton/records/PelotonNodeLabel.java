package org.apache.hadoop.yarn.server.router.external.peloton.records;

import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class PelotonNodeLabel extends BaseRecord {

  public PelotonNodeLabel() {
    super();
  }

  public static PelotonNodeLabel newInstance() {
    PelotonNodeLabel pelotonNodeLabel = StateStoreSerializer.newRecord(PelotonNodeLabel.class);
    pelotonNodeLabel.init();
    return pelotonNodeLabel;
  }

  public static PelotonNodeLabel newInstance(String nodeLabel) {
    PelotonNodeLabel pelotonNodeLabel = newInstance();
    pelotonNodeLabel.setNodeLabel(nodeLabel);
    return pelotonNodeLabel;
  }

  public abstract String getNodeLabel();
  public abstract void setNodeLabel(String nodeLabel);

  @Override
  public Map<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("nodeLabel", getNodeLabel());
    return map;
  }

  @Override
  public String toString() {
    return getNodeLabel();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PelotonNodeLabel) {
      return getNodeLabel().equals(((PelotonNodeLabel) obj).getNodeLabel());
    }
    return false;
  }
  @Override
  public int compareTo(BaseRecord other) {
    if (other == null) {
      return -1;
    } else if (other instanceof PelotonNodeLabel) {
      PelotonNodeLabel pelotonNodeLabel = (PelotonNodeLabel) other;
      return this.getNodeLabel().compareTo(pelotonNodeLabel.getNodeLabel());
    } else {
      return super.compareTo(other);
    }
  }
}
