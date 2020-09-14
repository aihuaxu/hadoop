package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.ClearAllPelotonZKConfsResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKInfoListByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKConfByClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.RemovePelotonZKInfoFromClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKConfResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonZKInfoToClusterResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonZKConfListResponse;

import java.io.IOException;

public interface PelotonZKConfManager {
  /**
   * Get all clusters' Peloton ZK configuration
   * @param request request
   * @return response
   * @throws IOException
   */
  GetPelotonZKConfListResponse getPelotonZKConfList(GetPelotonZKConfListRequest request) throws IOException;

  /**
   * Save(create/update) Peloton ZK configuration List
   * @param request request
   * @return response
   * @throws IOException
   */
  SavePelotonZKConfResponse savePelotonZKConf(SavePelotonZKConfRequest request) throws IOException;

  /**
   * Remove Peloton ZK configuration from specific cluster
   * @param request request
   * @return response
   * @throws IOException
   */
  RemovePelotonZKConfByClusterResponse removePelotonZKConfByCluster(RemovePelotonZKConfByClusterRequest request) throws IOException;

  /**
   * Clear all Peloton ZK configuration
   * @param request request
   * @return response
   * @throws IOException
   */
  ClearAllPelotonZKConfsResponse clearAllPelotonZKConfs(ClearAllPelotonZKConfsRequest request) throws IOException;

  /**
   * Get Peloton ZK configuration by specific cluster
   * @param request request
   * @return response
   * @throws IOException
   */
  GetPelotonZKInfoListByClusterResponse getPelotonZKConfListByCluster(GetPelotonZKInfoListByClusterRequest request) throws IOException;

  /**
   * Save(create/update) Peloton ZK configuration in specific cluster
   * @param request request
   * @return response
   * @throws IOException
   */
  SavePelotonZKInfoToClusterResponse savePelotonZKInfoToCluster(SavePelotonZKInfoToClusterRequest request) throws IOException;

  /**
   * Remove Peloton ZK configuration in specific cluster
   * @param request request
   * @return response
   * @throws IOException
   */
  RemovePelotonZKInfoFromClusterResponse removePelotonZKInfoFromCluster(RemovePelotonZKInfoFromClusterRequest request) throws IOException;
}
