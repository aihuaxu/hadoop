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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TestFsVolumeList {

  private final Configuration conf = new Configuration();
  private VolumeChoosingPolicy<FsVolumeImpl> blockChooser =
      new RoundRobinVolumeChoosingPolicy<>();
  private FsDatasetImpl dataset = null;
  private String baseDir;
  private BlockScanner blockScanner;

  @Before
  public void setUp() {
    dataset = mock(FsDatasetImpl.class);
    baseDir = new FileSystemTestHelper().getTestRootDir();
    Configuration blockScannerConf = new Configuration();
    blockScannerConf.setInt(DFSConfigKeys.
        DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    blockScanner = new BlockScanner(null, blockScannerConf);
  }

  @Test(timeout=30000)
  public void testGetNextVolumeWithClosedVolume() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), blockScanner, blockChooser, conf);
    final List<FsVolumeImpl> volumes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      File curDir = new File(baseDir, "nextvolume-" + i);
      curDir.mkdirs();
      FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", curDir,
          conf, StorageType.DEFAULT);
      volume.setCapacityForTesting(1024 * 1024 * 1024);
      volumes.add(volume);
      volumeList.addVolume(volume.obtainReference());
    }

    // Close the second volume.
    volumes.get(1).setClosed();
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return volumes.get(1).checkClosed();
        }
      }, 100, 3000);
    } catch (TimeoutException e) {
      fail("timed out while waiting for volume to be removed.");
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    for (int i = 0; i < 10; i++) {
      try (FsVolumeReference ref =
          volumeList.getNextVolume(StorageType.DEFAULT, 128)) {
        // volume No.2 will not be chosen.
        assertNotEquals(ref.getVolume(), volumes.get(1));
      }
    }
  }

  @Test(timeout=30000)
  public void testCheckDirsWithClosedVolume() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), blockScanner, blockChooser, conf);
    final List<FsVolumeImpl> volumes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      File curDir = new File(baseDir, "volume-" + i);
      curDir.mkdirs();
      FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", curDir,
          conf, StorageType.DEFAULT);
      volumes.add(volume);
      volumeList.addVolume(volume.obtainReference());
    }

    // Close the 2nd volume.
    volumes.get(1).setClosed();
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return volumes.get(1).checkClosed();
        }
      }, 100, 3000);
    } catch (TimeoutException e) {
      fail("timed out while waiting for volume to be removed.");
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    // checkDirs() should ignore the 2nd volume since it is closed.
    volumeList.checkDirs();
  }

  @Test(timeout=30000)
  public void testReleaseVolumeRefIfNoBlockScanner() throws IOException {
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(), null, blockChooser, conf);
    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", volDir,
        conf, StorageType.DEFAULT);
    FsVolumeReference ref = volume.obtainReference();
    volumeList.addVolume(ref);
    assertNull(ref.getVolume());
  }

  @Test
  public void testDfsReservedForDifferentStorageTypes() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, 100L);

    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    // when storage type reserved is not configured,should consider
    // dfs.datanode.du.reserved.
    FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", volDir, conf,
        StorageType.RAM_DISK);
    assertEquals("", 100L, volume.getReserved());
    // when storage type reserved is configured.
    conf.setLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY + "."
            + StringUtils.toLowerCase(StorageType.RAM_DISK.toString()), 1L);
    conf.setLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY + "."
            + StringUtils.toLowerCase(StorageType.SSD.toString()), 2L);
    FsVolumeImpl volume1 = new FsVolumeImpl(dataset, "storage-id", volDir,
        conf, StorageType.RAM_DISK);
    assertEquals("", 1L, volume1.getReserved());
    FsVolumeImpl volume2 = new FsVolumeImpl(dataset, "storage-id", volDir,
        conf, StorageType.SSD);
    assertEquals("", 2L, volume2.getReserved());
    FsVolumeImpl volume3 = new FsVolumeImpl(dataset, "storage-id", volDir,
        conf, StorageType.DISK);
    assertEquals("", 100L, volume3.getReserved());
    FsVolumeImpl volume4 = new FsVolumeImpl(dataset, "storage-id", volDir,
        conf, StorageType.DEFAULT);
    assertEquals("", 100L, volume4.getReserved());
  }

  @Test
  public void testNonDfsUsedMetricForVolume() throws Exception {
    File volDir = new File(baseDir, "volume-0");
    volDir.mkdirs();
    /*
     * Lets have the example.
     * Capacity - 1000
     * Reserved - 100
     * DfsUsed  - 200
     * Actual Non-DfsUsed - 300 -->(expected)
     * ReservedForReplicas - 50
     */
    long diskCapacity = 1000L;
    long duReserved = 100L;
    long dfsUsage = 200L;
    long actualNonDfsUsage = 300L;
    long reservedForReplicas = 50L;
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, duReserved);
    FsVolumeImpl volume = new FsVolumeImpl(dataset, "storage-id", volDir, conf,
        StorageType.DEFAULT);
    FsVolumeImpl spyVolume = Mockito.spy(volume);
    // Set Capacity for testing
    long testCapacity = diskCapacity - duReserved;
    spyVolume.setCapacityForTesting(testCapacity);
    // Mock volume.getDfAvailable()
    long dfAvailable = diskCapacity - dfsUsage - actualNonDfsUsage;
    Mockito.doReturn(dfAvailable).when(spyVolume).getDfAvailable();
    // Mock dfsUsage
    Mockito.doReturn(dfsUsage).when(spyVolume).getDfsUsed();
    // Mock reservedForReplcas
    Mockito.doReturn(reservedForReplicas).when(spyVolume)
        .getReservedForReplicas();
    Mockito.doReturn(actualNonDfsUsage).when(spyVolume)
        .getActualNonDfsUsed();
    long expectedNonDfsUsage =
        actualNonDfsUsage - duReserved;
    assertEquals(expectedNonDfsUsage, spyVolume.getNonDfsUsed());
  }


  // Test basics with same disk archival turned on.
  @Test
  public void testGetVolumeWithSameDiskArchival() throws Exception {
    File diskVolDir = new File(baseDir,
        "volume-disk/current");
    File archivalVolDir = new File(baseDir,
        "volume-archival/current");
    diskVolDir.mkdirs();
    archivalVolDir.mkdirs();
    double reservedForArchival = 0.75;
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING,
        true);
    conf.setDouble(DFSConfigKeys
            .DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE,
        reservedForArchival);

    FsVolumeImpl diskVolume = new FsVolumeImpl(dataset,
        "storage-id", diskVolDir,
        conf, StorageType.DEFAULT);
    FsVolumeImpl archivalVolume = new FsVolumeImpl(dataset,
        "storage-id", archivalVolDir,
        conf, StorageType.ARCHIVE);
    FsVolumeList volumeList = new FsVolumeList(
        Collections.<VolumeFailureInfo>emptyList(),
        blockScanner, blockChooser, conf);
    volumeList.addVolume(archivalVolume.obtainReference());
    volumeList.addVolume(diskVolume.obtainReference());

    assertEquals(diskVolume.getMount(), archivalVolume.getMount());
    String device = diskVolume.getMount();

    // 1) getVolumeRef should return correct reference.
    assertEquals(diskVolume,
        volumeList.getMountVolumeMap()
            .getVolumeRefByMountAndStorageType(
                device, StorageType.DISK).getVolume());
    assertEquals(archivalVolume,
        volumeList.getMountVolumeMap()
            .getVolumeRefByMountAndStorageType(
                device, StorageType.ARCHIVE).getVolume());

    // 2) removeVolume should work as expected
    volumeList.removeVolume(new File(diskVolume.getBasePath()), true);
    assertNull(volumeList.getMountVolumeMap()
        .getVolumeRefByMountAndStorageType(
            device, StorageType.DISK));
    assertEquals(archivalVolume, volumeList.getMountVolumeMap()
        .getVolumeRefByMountAndStorageType(
            device, StorageType.ARCHIVE).getVolume());
  }

  // Test dfs stats with same disk archival
  @Test
  public void testDfsUsageStatWithSameDiskArchival() throws Exception {
    File diskVolDir = new File(baseDir, "volume-disk");
    File archivalVolDir = new File(baseDir, "volume-archival");
    diskVolDir.mkdirs();
    archivalVolDir.mkdirs();

    long dfCapacity = 1100L;
    double reservedForArchival = 0.75;
    // Disk and Archive shares same du Reserved.
    long duReserved = 100L;
    long diskDfsUsage = 100L;
    long archivalDfsUsage = 200L;
    long dfUsage = 700L;
    long dfAvailable = 300L;

    // Set up DISK and ARCHIVAL and capacity.
    conf.setLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY, duReserved);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING,
        true);
    conf.setDouble(DFSConfigKeys
            .DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE,
        reservedForArchival);
    FsVolumeImpl diskVolume = new FsVolumeImpl(dataset,
        "storage-id", diskVolDir,
        conf, StorageType.DEFAULT);
    FsVolumeImpl archivalVolume = new FsVolumeImpl(dataset,
        "storage-id", archivalVolDir,
        conf, StorageType.ARCHIVE);
    FsVolumeImpl spyDiskVolume = Mockito.spy(diskVolume);
    FsVolumeImpl spyArchivalVolume = Mockito.spy(archivalVolume);
    long testDfCapacity = dfCapacity - duReserved;
    spyDiskVolume.setCapacityForTesting(testDfCapacity);
    spyArchivalVolume.setCapacityForTesting(testDfCapacity);
    Mockito.doReturn(dfAvailable).when(spyDiskVolume).getDfAvailable();
    Mockito.doReturn(dfAvailable).when(spyArchivalVolume).getDfAvailable();

    MountVolumeMap mountVolumeMap = new MountVolumeMap(conf);
    mountVolumeMap.addVolume(spyDiskVolume);
    mountVolumeMap.addVolume(spyArchivalVolume);
    Mockito.doReturn(mountVolumeMap).when(dataset).getMountVolumeMap();

    // 1) getCapacity() should reflect configured archive storage percentage.
    long diskStorageTypeCapacity =
        (long) ((dfCapacity - duReserved) * (1 - reservedForArchival));
    assertEquals(diskStorageTypeCapacity, spyDiskVolume.getCapacity());
    long archiveStorageTypeCapacity =
        (long) ((dfCapacity - duReserved) * (reservedForArchival));
    assertEquals(archiveStorageTypeCapacity, spyArchivalVolume.getCapacity());

    // 2) getActualNonDfsUsed() should count in both DISK and ARCHIVE.
    // expectedActualNonDfsUsage =
    // diskUsage - archivalDfsUsage - diskDfsUsage
    long expectedActualNonDfsUsage = 400L;
    Mockito.doReturn(diskDfsUsage)
        .when(spyDiskVolume).getDfsUsed();
    Mockito.doReturn(archivalDfsUsage)
        .when(spyArchivalVolume).getDfsUsed();
    Mockito.doReturn(dfUsage)
        .when(spyDiskVolume).getDfUsed();
    Mockito.doReturn(dfUsage)
        .when(spyArchivalVolume).getDfUsed();
    assertEquals(expectedActualNonDfsUsage,
        spyDiskVolume.getActualNonDfsUsed());
    assertEquals(expectedActualNonDfsUsage,
        spyArchivalVolume.getActualNonDfsUsed());

    // 3) When there is only one volume on a disk mount,
    // we allocate the full disk capacity regardless of the default ratio.
    mountVolumeMap.removeVolume(spyArchivalVolume);
    assertEquals(dfCapacity - duReserved, spyDiskVolume.getCapacity());
  }
}
