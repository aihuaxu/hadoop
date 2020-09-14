package org.apache.hadoop.yarn.server.router.external.peloton.records;

import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class PelotonZKConf extends BaseRecord {
  public PelotonZKConf() {
    super();
  }
  public static PelotonZKConf newInstance() {
    PelotonZKConf zkConf = StateStoreSerializer.newRecord(PelotonZKConf.class);
    zkConf.init();
    return zkConf;
  }

  public static PelotonZKConf newInstance(String cluster, List<PelotonZKInfo> zkInfoList) {
    PelotonZKConf zkConf = newInstance();

    zkConf.setCluster(cluster);
    zkConf.setPelotonZKInfo(zkInfoList);
    return zkConf;
  }

  public abstract String getCluster();
  public abstract void setCluster(String cluster);
  public abstract List<PelotonZKInfo> getPelotonZKInfoList();
  public abstract void setPelotonZKInfo(List<PelotonZKInfo> zkInfoList);

  @Override
  public Map<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("cluster", getCluster());
    return map;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PelotonZKConf ) {
      PelotonZKConf other = (PelotonZKConf) obj;
      if (!this.getCluster().equals(other.getCluster())) {
        return false;
      } else
        return this.getPelotonZKInfoList().equals(other.getPelotonZKInfoList());
    }
    return false;
  }

  @Override
  public int compareTo(BaseRecord other) {
    if (other == null) {
      return -1;
    } else if (other instanceof PelotonZKConf) {
      PelotonZKConf pelotonZKConf = (PelotonZKConf) other;
      return this.getCluster().compareTo(pelotonZKConf.getCluster());
    } else {
      return super.compareTo(other);
    }
  }
}
