package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.GetPelotonNodeLabelResponse;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelRequest;
import org.apache.hadoop.yarn.server.router.external.peloton.protocol.SavePelotonNodeLabelResponse;

import java.io.IOException;

public interface PelotonNodeLabelManager {
  /**
   * Get node label for peloton hosts
   */
  GetPelotonNodeLabelResponse getPelotonNodeLabel(GetPelotonNodeLabelRequest request) throws IOException;

  /**
   * Set node label for peloton hosts
   */
  SavePelotonNodeLabelResponse savePelotonNodeLabel(SavePelotonNodeLabelRequest request) throws IOException;
}
