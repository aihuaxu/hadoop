package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;
import peloton.api.v1alpha.peloton.Peloton;
import peloton.api.v1alpha.pod.Pod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPelotonJobSpec {
  Configuration conf;

  @Test
  public void testGetPodLabel() {
    List<Peloton.Label> labels = PelotonJobSpec.getPodLabels();
    List<String> podLabelKey = new ArrayList<>(PelotonJobSpec.POD_LABEL_MAP.keySet());
    for (int i = 0; i < labels.size(); i++) {
      String targetKey = podLabelKey.get(i);
      String targetValue = PelotonJobSpec.POD_LABEL_MAP.get(targetKey);
      Peloton.Label actualLabel = labels.get(i);
      assertEquals(targetKey, actualLabel.getKey());
      assertEquals(targetValue, actualLabel.getValue());
    }
  }

  @Test
  public void testGetJobLabel() {
    Iterator<Peloton.Label> labelIterator = PelotonJobSpec.getLabels().iterator();
    List<String> labelKeys = new ArrayList<>(PelotonJobSpec.LABEL_MAP.keySet());
    int labelIterableIndex = 0;
    while (labelIterator.hasNext()) {
      String targetKey = labelKeys.get(labelIterableIndex);
      Peloton.Label actualLabel = labelIterator.next();
      assertEquals(targetKey, actualLabel.getKey());
      labelIterableIndex++;
    }
  }

  @Test
  public void testGetContainerSpec() throws IOException {
    conf = new Configuration();
    conf.set(YarnConfiguration.PELOTON_DOCKER_IMAGE_NAME, "test-image");
    conf.set(YarnConfiguration.PELOTON_DOCKER_IMAGE_TAG, ":test-tag");
    Pod.ContainerSpec actualContainerSpec = PelotonJobSpec.getContainerSpec(conf);
    assertEquals(PelotonJobSpec.Constants.CONTAINER_INFO_NAME, actualContainerSpec.getName());
    assertEquals("test-image:test-tag", actualContainerSpec.getImage());
    Pod.CommandSpec actualCommandSpec = actualContainerSpec.getEntrypoint();
    assertEquals(PelotonJobSpec.Constants.POD_SPEC_SHELL, actualCommandSpec.getValue());
    assertEquals(PelotonJobSpec.Constants.POD_SPEC_CPU_LIMIT, actualContainerSpec.getResource().getCpuLimit(), 0.002);
    assertEquals(PelotonJobSpec.Constants.POD_SPEC_MEM_LIMIT, actualContainerSpec.getResource().getMemLimitMb(), 0.002);
    assertEquals(PelotonJobSpec.Constants.POD_SPEC_DISK_LIMIT, actualContainerSpec.getResource().getDiskLimitMb(), 0.002);
    List<Pod.VolumeMount> volumeMounts = actualContainerSpec.getVolumeMountsList();
    for(int i = 0; i < volumeMounts.size(); i++) {
      Pod.VolumeMount vm = volumeMounts.get(i);
      assertEquals(VolumeSpec.VOLUMES_NAME[i], vm.getName());
      assertEquals(VolumeSpec.VOLUMES_MOUNTS[i], vm.getMountPath());
    }
  }

  @Test(expected = IOException.class)
  public void testMissingImage() throws IOException {
    conf = new Configuration();
    Pod.ContainerSpec actualContainerSpec = PelotonJobSpec.getContainerSpec(conf);
    assertEquals(PelotonJobSpec.Constants.CONTAINER_INFO_NAME, actualContainerSpec.getName());
  }
}
