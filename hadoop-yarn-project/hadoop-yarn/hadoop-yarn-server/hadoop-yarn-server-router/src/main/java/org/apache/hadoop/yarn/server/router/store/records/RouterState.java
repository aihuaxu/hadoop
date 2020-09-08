package org.apache.hadoop.yarn.server.router.store.records;

import org.apache.hadoop.store.record.BaseRecord;
import org.apache.hadoop.yarn.server.router.RouterServiceState;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.apache.hadoop.yarn.server.router.store.driver.StateStoreSerializer;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Entry to log the state of a
 * {@link org.apache.hadoop.yarn.server.router.Router Router} in the
 * {@link RouterStateStoreService
 * FederationStateStoreService}.
 */
public abstract class RouterState extends BaseRecord {

  /** Expiration time in ms for this entry. */
  private static long expirationMs;

  /**
   * Constructors.
   */
  public RouterState() {
    super();
  }

  public static RouterState newInstance() {
    RouterState record = StateStoreSerializer.newRecord(RouterState.class);
    record.init();
    return record;
  }

  public static RouterState newInstance(String addr, long startTime,
      RouterServiceState status) {
    RouterState record = newInstance();
    record.setDateStarted(startTime);
    record.setAddress(addr);
    record.setStatus(status);
    return record;
  }

  public abstract void setAddress(String address);

  public abstract void setDateStarted(long dateStarted);

  public abstract String getAddress();

  public abstract RouterServiceState getStatus();

  public abstract void setStatus(RouterServiceState newStatus);

  public abstract long getDateStarted();

  /**
   * Get the identifier for the Router. It uses the address.
   *
   * @return Identifier for the Router.
   */
  public String getRouterId() {
    return getAddress();
  }

  @Override
  public boolean like(BaseRecord o) {
    if (o instanceof RouterState) {
      RouterState other = (RouterState)o;
      if (getAddress() != null &&
          !getAddress().equals(other.getAddress())) {
        return false;
      }
      if (getStatus() != null &&
          !getStatus().equals(other.getStatus())) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return getAddress() + " -> " + getStatus();
  }

  @Override
  public SortedMap<String, String> getPrimaryKeys() {
    SortedMap<String, String> map = new TreeMap<>();
    map.put("address", getAddress());
    return map;
  }

  @Override
  public void validate() {
    super.validate();
    if ((getAddress() == null || getAddress().length() == 0) &&
        getStatus() != RouterServiceState.INITIALIZING) {
      throw new IllegalArgumentException(
          "Invalid router entry, no address specified " + this);
    }
  }

  @Override
  public int compareTo(BaseRecord other) {
    if (other == null) {
      return -1;
    } else if (other instanceof RouterState) {
      RouterState router = (RouterState) other;
      return this.getAddress().compareTo(router.getAddress());
    } else {
      return super.compareTo(other);
    }
  }

  @Override
  public boolean checkExpired(long currentTime) {
    if (super.checkExpired(currentTime)) {
      setStatus(RouterServiceState.EXPIRED);
      return true;
    }
    return false;
  }

  @Override
  public long getExpirationMs() {
    return RouterState.expirationMs;
  }

  public static void setExpirationMs(long time) {
    RouterState.expirationMs = time;
  }
}

