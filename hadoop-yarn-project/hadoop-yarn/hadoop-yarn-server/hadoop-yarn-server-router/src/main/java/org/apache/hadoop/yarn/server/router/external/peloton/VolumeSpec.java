package org.apache.hadoop.yarn.server.router.external.peloton;

import peloton.api.v1alpha.volume.Volume;

import java.util.ArrayList;
import java.util.List;

public class VolumeSpec {
  /**
   * Volume initializations
   * Note: Add new entries in the order
   */
  private String volumeName;
  private String volumeHostPath;
  private String volumeMount;
  private boolean volumeMountScope;
  private Volume.VolumeSpec.VolumeType volumeType;

  /**
   * Ordered list for volumes
   */
  public static final String[] VOLUMES_NAME = {
          "secrets",
          "docker.sock",
          "config",
          "data",
          "tmp",
          "nm-log",
          "var-cache-ucs",
          "shared",
          "langley"
  };

  public static final String[] VOLUMES_HOST_PATH = {
          "/secrets/compute/yarn",
          "/var/run/docker.sock",
          "/opt/uber/shared/yarn/cache/config",
          "/opt/uber/shared/yarn/cache/data",
          "/opt/uber/shared/yarn/cache/tmp",
          "/opt/uber/shared/yarn/log",
          "/var/cache/ucs",
          "/opt/uber/shared/yarn/cache/yarn-shared",
          "/langley"
  };

  public static final String[] VOLUMES_MOUNTS = {
          "/secrets",
          "/var/run/docker.sock",
          "/config",
          "/data",
          "/tmp",
          "/log",
          "/var/cache/ucs",
          "/yarn-shared",
          "/langley"
  };

  // whether this mount is read-only or not
  // true: ro
  // false: rw
  public static final boolean[] VOLUMES_MOUNTS_SCOPE = {
          false,
          true,
          true,
          false,
          false,
          false,
          false,
          false,
          false,
          false,
  };

  /**
   * Static initialization
   */
  private static final List<VolumeSpec> volumes;
  static {
    volumes = new ArrayList<>();
    for (int i = 0; i < VOLUMES_NAME.length; i++) {
      volumes.add(new VolumeSpec(VOLUMES_NAME[i],
              VOLUMES_HOST_PATH[i], VOLUMES_MOUNTS[i],
              VOLUMES_MOUNTS_SCOPE[i],
              Volume.VolumeSpec.VolumeType.VOLUME_TYPE_HOST_PATH));
    }
  }

  public VolumeSpec(String volumeName, String volumeHostPath,
                    String volumeMount, boolean volumeMountScope,
                    Volume.VolumeSpec.VolumeType volumeType) {

    this.volumeName = volumeName;
    this.volumeHostPath = volumeHostPath;
    this.volumeMount = volumeMount;
    this.volumeMountScope = volumeMountScope;
    this.volumeType = volumeType;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getVolumeHostPath() {
    return volumeHostPath;
  }

  public String getVolumeMount() {
    return volumeMount;
  }

  public boolean getVolumeMountScope() {
    return volumeMountScope;
  }

  public Volume.VolumeSpec.VolumeType getVolumeType() {
    return volumeType;
  }

  /**
   * Read only copy
   * @return
   */
  public static List<VolumeSpec> getAllVolumes() {
    return new ArrayList<>(volumes);
  }
}
