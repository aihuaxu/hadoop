package org.apache.hadoop.yarn.server.router.external.peloton.records;

import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class PelotonZKInfo extends BaseRecord {
  public PelotonZKInfo() {
    super();
  }

  public static PelotonZKInfo newInstance() {
    PelotonZKInfo zkInfo = StateStoreSerializer.newRecord(PelotonZKInfo.class);
    zkInfo.init();
    return zkInfo;
  }

  public static PelotonZKInfo newInstance(String zkAddress) {
    PelotonZKInfo zkInfo = newInstance();
    zkInfo.setZKAddress(zkAddress);
    return zkInfo;
  }

  public static PelotonZKInfo newInstance(String zone, String region, String zkAddress) {
    PelotonZKInfo zkInfo = newInstance();
    zkInfo.setZone(zone);
    zkInfo.setRegion(region);
    zkInfo.setZKAddress(zkAddress);
    return zkInfo;
  }

  public abstract String getZone();
  public abstract void setZone(String zone);
  public abstract String getRegion();
  public abstract void setRegion(String region);
  public abstract String getZKAddress();
  public abstract void setZKAddress(String zkAddress);

  @Override
  public Map<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("zkAddress", getZKAddress());
    return map;
  }

  @Override
  public String toString() {
    return getZone() + ":" + getRegion() + ":" + getZKAddress();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PelotonZKInfo) {
      if (getZKAddress().equals(((PelotonZKInfo) obj).getZKAddress())) {
        return true;
      } else {
        PelotonZKInfo other = (PelotonZKInfo) obj;
        return this.toString().equals(other.toString());
      }
    }
    return false;
  }
  @Override
  public int compareTo(BaseRecord other) {
    if (other == null) {
      return -1;
    } else if (other instanceof PelotonZKInfo) {
      PelotonZKInfo pelotonZKInfo = (PelotonZKInfo) other;
      return this.toString().compareTo(pelotonZKInfo.toString());
    } else {
      return super.compareTo(other);
    }
  }

}