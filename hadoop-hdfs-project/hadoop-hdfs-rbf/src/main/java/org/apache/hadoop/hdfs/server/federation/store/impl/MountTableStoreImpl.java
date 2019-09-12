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
package org.apache.hadoop.hdfs.server.federation.store.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterPermissionChecker;
import org.apache.hadoop.hdfs.server.federation.store.MountTableStore;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.Query;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link MountTableStore} state store API.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MountTableStoreImpl extends MountTableStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(MountTableStoreImpl.class);

  private static final String MOUNTTABLE_ENTRY_ADD_MESSAGE =
      "Added a mounttable entry %s with status %s";
  private static final String MOUNTTABLE_ENTRY_UPDATE_MESSAGE =
      "Updated a mounttable entry from %s to %s with status %s";
  private static final String MOUNTTABLE_ENTRY_REMOVE_MESSAGE =
      "Deleted a mounttable entry %s with status %s";

  public MountTableStoreImpl(StateStoreDriver driver) {
    super(driver);
  }

  @Override
  public AddMountTableEntryResponse addMountTableEntry(
      AddMountTableEntryRequest request) throws IOException {
    MountTable mountTable = request.getEntry();
    if (mountTable != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(mountTable, FsAction.WRITE);
      }
    }

    boolean status = getDriver().put(mountTable, false, true);
    LOG.info(String.format(MOUNTTABLE_ENTRY_ADD_MESSAGE,
        mountTable.toString(), status));
    AddMountTableEntryResponse response =
        AddMountTableEntryResponse.newInstance();
    response.setStatus(status);
    return response;
  }

  @Override
  public UpdateMountTableEntryResponse updateMountTableEntry(
      UpdateMountTableEntryRequest request) throws IOException {
    MountTable mountTable = request.getEntry();
    if (mountTable != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(mountTable, FsAction.WRITE);
      }
    }
    // get current source -> destinations first before rewriting
    final MountTable currentPartialEntry = MountTable.newInstance();
    currentPartialEntry.setSourcePath(mountTable.getSourcePath());
    final Query<MountTable> query = new Query<>(currentPartialEntry);
    final MountTable updatedEntry = getDriver().get(getRecordClass(), query);

    boolean status = getDriver().put(mountTable, true, true);
    if (updatedEntry != null) {
      LOG.info(String.format(MOUNTTABLE_ENTRY_UPDATE_MESSAGE,
          updatedEntry.toString(), mountTable.toString(), status));
    } else {
      // an entry-add happened since no previous entry exists
      LOG.info(String.format(MOUNTTABLE_ENTRY_ADD_MESSAGE,
          mountTable.toString(), status));
    }

    UpdateMountTableEntryResponse response =
        UpdateMountTableEntryResponse.newInstance();
    response.setStatus(status);
    return response;
  }

  @Override
  public RemoveMountTableEntryResponse removeMountTableEntry(
      RemoveMountTableEntryRequest request) throws IOException {
    final String srcPath = request.getSrcPath();
    final MountTable partial = MountTable.newInstance();
    partial.setSourcePath(srcPath);
    final Query<MountTable> query = new Query<>(partial);
    final MountTable deleteEntry = getDriver().get(getRecordClass(), query);

    boolean status = false;
    if (deleteEntry != null) {
      RouterPermissionChecker pc = RouterAdminServer.getPermissionChecker();
      if (pc != null) {
        pc.checkPermission(deleteEntry, FsAction.WRITE);
      }
      status = getDriver().remove(deleteEntry);
      LOG.info(String.format(MOUNTTABLE_ENTRY_REMOVE_MESSAGE,
          deleteEntry.toString(), status));
    }

    RemoveMountTableEntryResponse response =
        RemoveMountTableEntryResponse.newInstance();
    response.setStatus(status);
    return response;
  }

  @Override
  public GetMountTableEntriesResponse getMountTableEntries(
      GetMountTableEntriesRequest request) throws IOException {
    RouterPermissionChecker pc =
        RouterAdminServer.getPermissionChecker();
    // Get all values from the cache
    List<MountTable> records = getCachedRecords();

    // Sort and filter
    Collections.sort(records, MountTable.SOURCE_COMPARATOR);
    String reqSrcPath = request.getSrcPath();
    if (reqSrcPath != null && !reqSrcPath.isEmpty()) {
      // Return only entries beneath this path
      Iterator<MountTable> it = records.iterator();
      while (it.hasNext()) {
        MountTable record = it.next();
        String srcPath = record.getSourcePath();
        if (!srcPath.startsWith(reqSrcPath)) {
          it.remove();
        } else if (pc != null) {
          // do the READ permission check
          try {
            pc.checkPermission(record, FsAction.READ);
          } catch (AccessControlException ignored) {
            // Remove this mount table entry if it cannot
            // be accessed by current user.
            it.remove();
          }
        }
      }
    }

    GetMountTableEntriesResponse response =
        GetMountTableEntriesResponse.newInstance();
    response.setEntries(records);
    response.setTimestamp(Time.now());
    return response;
  }
}