package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peloton.api.v0.respool.ResourceManagerGrpc;
import peloton.api.v0.respool.Respool;
import peloton.api.v1alpha.job.stateless.Stateless;
import peloton.api.v1alpha.peloton.Peloton;
import peloton.api.v1alpha.pod.Pod;
import peloton.api.v1alpha.pod.apachemesos.Apachemesos;
import peloton.api.v1alpha.volume.Volume;

import java.io.IOException;
import java.util.*;

public class PelotonJobSpec {
  private final static Logger LOG =
          LoggerFactory.getLogger(PelotonJobSpec.class);

  /**
   * Job label initialization
   */
  public static final Map<String, String> LABEL_MAP;
  static {
    LABEL_MAP = new HashMap<>();
    LABEL_MAP.put("YARNService", "NodeManager");
  }

  /**
   * Pod spec labels initialization
   */
  public static final Map<String, String> POD_LABEL_MAP;
  static {
    POD_LABEL_MAP = new HashMap<>();
    POD_LABEL_MAP.put("YARNService", "NodeManager");
    POD_LABEL_MAP.put("com.uber.peloton.metadata.nodefaultmaximumunavailableinstances", "true");
    // Set of labels required for CLH to fetch pool secrets and keytab.
    POD_LABEL_MAP.put("org.apache.aurora.metadata.udeploy", "{\"svc_pconfig_dict\":{\"tier\":2}}");
    POD_LABEL_MAP.put("org.apache.aurora.metadata.udeploy_task", "{\"application_id\":\"yarn\",\"service_id\":\"yarn\"}");
    POD_LABEL_MAP.put("org.apache.aurora.metadata.usecrets.mount", "{\"container_path\":\"/secrets\",\"host_path\":\"/run/.secrets/yarn\"}");
    POD_LABEL_MAP.put("com.uber.peloton.metadata.usecrets.pool", "{\"pool\": \"hadoop_main\"}");
    POD_LABEL_MAP.put("com.uber.peloton.metadata.kerberos.keytab", "{\"principals\": [\"yarn/{HOST_NAME}.prod.uber.internal\"]}");
    POD_LABEL_MAP.put("org.apache.aurora.metadata.usecrets.regional", "true");
    POD_LABEL_MAP.put("org.apache.aurora.metadata.usecrets.enable", "true");
  }

  /**
   * Docker initialization
   */
  public static final Map<String, String> ENVIRONMENT_PARAM_MAP;
  static {
    ENVIRONMENT_PARAM_MAP = new HashMap<>();
    ENVIRONMENT_PARAM_MAP.put("UDEPLOY_APP_ID", "yarn-container-nm");
    ENVIRONMENT_PARAM_MAP.put("CONTAINER_TYPE", "nm");
    ENVIRONMENT_PARAM_MAP.put("EXTERNAL_LOG_PATH", "/opt/uber/shared/yarn/log");
    ENVIRONMENT_PARAM_MAP.put("INTERNAL_LOG_PATH", "/log");
    ENVIRONMENT_PARAM_MAP.put("EXTERNAL_DATA_PATH", "/opt/uber/shared/yarn/cache/data");
    ENVIRONMENT_PARAM_MAP.put("INTERNAL_DATA_PATH", "/data");
    ENVIRONMENT_PARAM_MAP.put("EXTERNAL_SHARED_PATH", "/opt/uber/shared/yarn/cache/yarn-shared");
    ENVIRONMENT_PARAM_MAP.put("INTERNAL_SHARED_PATH", "/yarn-shared");
    ENVIRONMENT_PARAM_MAP.put("UBER_RUNTIME_ENVIRONMENT", "production");
    ENVIRONMENT_PARAM_MAP.put("YARN_HOST_TYPE", "peloton");
    ENVIRONMENT_PARAM_MAP.put("EXTERNAL_SECRETS_ROOT", "/run/.secrets/yarn");
    ENVIRONMENT_PARAM_MAP.put("EXTERNAL_SECRETS_PATH", "/run/.secrets/yarn");
    ENVIRONMENT_PARAM_MAP.put("INTERNAL_SECRETS_PATH", "/secrets");
  }

  /**
   * Mesos Spec
   *
   */
  public static final Map<String, String> MESOS_DOCKER_PARAMS;
  static {
    MESOS_DOCKER_PARAMS = new HashMap<>();
    MESOS_DOCKER_PARAMS.put("pid", "host");
    MESOS_DOCKER_PARAMS.put("cgroup-parent", "/yarn-apps");
  }

    /**
     * Constant literals
     */
  class Constants {
    /**
     * JOB related
     */
    public static final String JOB_NAME = "YARN-NM";
    public static final String OWNING_TEAM = "hadoop";
    public static final String LDAP_GROUPS = "hadoop-dev";
    public static final String DESCRIPTION = "A stateless job of YARN NodeManager";

    /**
     * Label related only for service
     */
    public static final String LABEL_KEY_SERVICE = "YARNService";
    public static final String LABEL_VALUE_SERVICE = "NodeManager";

    /**
     * Peloton host pool
     */
    public static final String PELOTON_HOST_POOL_SHARED_TO_YARN = "shared";

    /**
     * Container Info
     * Image
     */
    public static final String CONTAINER_INFO_NAME =
            "container0";

    /**
     * Pod Spec
     * Resource limit
     *
     */
    public static final int POD_SPEC_CPU_LIMIT = 2;
    public static final int POD_SPEC_MEM_LIMIT = 50240;
    public static final int POD_SPEC_DISK_LIMIT = 10240;

    /**
     * Pod Spec
     * Command
     */
    public static final String POD_SPEC_SHELL = "/bin/bash";
    public static final String POD_SPEC_SHELL_VALUE = "/etc/udocker/entrypoint";

    /**
     * Pod constraints
     */
    public static final int POD_CONSTRAINT_TYPE_VALUE = 1;
    public static final int POD_CONSTRAINT_LABEL_CONSTRAINT_KIND_VALUE = 1;
    public static final int POD_CONSTRAINT_LABEL_CONSTRAINT_CONDITION_VALUE = 2;
    public static final int POD_CONSTRAINT_LABEL_CONSTRAINT_REQUIREMENT = 0;

    // Constant Keys
    public static final String UBER_REGION_ENVIRON_KEY = "UBER_REGION";
    public static final String UBER_ZONE_ENVIRON_KEY = "UBER_ZONE";
    public static final String ODIN_INSTANCE = "ODIN_INSTANCE";
  }

  /**
   * Return job name
   * @return
   */
  public static String getJobName() {
    return Constants.JOB_NAME;
  }

  /**
   * Return owning team
   * @return
   */
  public static String getOwningTeam() {
    return Constants.OWNING_TEAM;
  }

  public static String getLdapGroups() {
    return Constants.LDAP_GROUPS;
  }

  public static String getDescription() {
    return Constants.DESCRIPTION;
  }

  public static Peloton.Label getLabel() {
    return Peloton.Label.newBuilder().
            setKey(Constants.LABEL_KEY_SERVICE).
            setValue(Constants.LABEL_VALUE_SERVICE).
            build();
  }

  /**
   * Get Labels iterable
   * @return
   */
  public static Iterable<Peloton.Label> getLabels() {
    final List<Peloton.Label> labels = new ArrayList<>();
    for (String key : LABEL_MAP.keySet()) {
      Peloton.Label.Builder label = Peloton.Label.newBuilder();
      label.
              setKey(key).
              setValue(LABEL_MAP.get(key));
      labels.add(label.build());
    }
    return new Iterable<Peloton.Label>() {
      @Override
      public Iterator<Peloton.Label> iterator() {
        return labels.iterator();
      }
    };
  }

  /**
   * Get pod labels
   * @return
   */
  public static List<Peloton.Label> getPodLabels() {
    final List<Peloton.Label> labels = new ArrayList<>();
    for (String key : POD_LABEL_MAP.keySet()) {
      Peloton.Label.Builder label = Peloton.Label.newBuilder();
      label.
        setKey(key).
        setValue(POD_LABEL_MAP.get(key));
      labels.add(label.build());
    }
    return labels;
  }

  /**
   * Get respool id
   * @param resourceManager
   * @return
   */
  public static Peloton.ResourcePoolID getRespoolId
          (ResourceManagerGrpc.ResourceManagerBlockingStub resourceManager,
            String resourcePoolPath) {
    return Peloton.ResourcePoolID.newBuilder().
            setValue(lookupPool(resourceManager, resourcePoolPath)).
            build();
  }

  /**
   * Get host pool
   * @return
   */
  public static String getHostPool() {
    return Constants.PELOTON_HOST_POOL_SHARED_TO_YARN;
  }

  /**
   * Pod Spec
   * @return
   */
  public static Pod.PodSpec getPodSpec(Configuration conf) throws IOException {

    Pod.PodSpec.Builder podSpecBuilder = Pod.PodSpec.newBuilder();
    podSpecBuilder.
            addAllLabels(getPodLabels()).
            setMesosSpec(getMesosSpec()).
            setConstraint(getPodConstraint()).
            addContainers(getContainerSpec(conf));
    List<VolumeSpec> volumeSpecs = VolumeSpec.getAllVolumes();
    for (int i = 0; i < volumeSpecs.size(); i++) {
      podSpecBuilder.
              addVolumes(getVolume(volumeSpecs.get(i)));
    }
    return podSpecBuilder.build();
  }

  /**
   * Mesos spec
   * @return
   */
  public static Apachemesos.PodSpec getMesosSpec() {
    Apachemesos.PodSpec.Builder mesosSpec = Apachemesos.PodSpec.newBuilder();
    for (String key : MESOS_DOCKER_PARAMS.keySet()) {
      mesosSpec.addDockerParameters(getDockerParams(key));
    }
    mesosSpec.
            setExecutorSpec(
                    Apachemesos.PodSpec.ExecutorSpec.newBuilder().
                    setType(Apachemesos.PodSpec.ExecutorSpec.ExecutorType.EXECUTOR_TYPE_CUSTOM
                    ).
                            build()
            );
    return mesosSpec.build();
  }

  /**
   * Docker params for mesos spec
   * @param key
   * @return
   */
  public static Apachemesos.PodSpec.DockerParameter getDockerParams(String key) {
    Apachemesos.PodSpec.DockerParameter.Builder dockerParam =
            Apachemesos.PodSpec.DockerParameter.newBuilder();
    dockerParam.
            setKey(key).
            setValue(MESOS_DOCKER_PARAMS.get(key));
    return dockerParam.build();
  }
  /**
   * Get VolumeSpec
   * @param volumeSpec
   * @return
   */
  public static Volume.VolumeSpec getVolume(VolumeSpec volumeSpec) {
    Volume.VolumeSpec.Builder volumeSpecBuilder = Volume.VolumeSpec.newBuilder();
    volumeSpecBuilder.
            setName(volumeSpec.getVolumeName()).
            setHostPath(Volume.VolumeSpec.HostPathVolumeSource.newBuilder().
                    setPath(volumeSpec.getVolumeHostPath()).build()).
            setType(volumeSpec.getVolumeType());
    return volumeSpecBuilder.build();
  }

  /**
   * Constraint Spec
   * @return
   */
  public static Pod.Constraint getPodConstraint() {
    Pod.Constraint.Builder podConstraint = Pod.Constraint.newBuilder();
    podConstraint.
            setTypeValue(Constants.POD_CONSTRAINT_TYPE_VALUE).
            setLabelConstraint(getPodLabelConstraint());
    return podConstraint.build();
  }

  /**
   * Label Constraint Spec
   * @return
   */
  public static Pod.LabelConstraint getPodLabelConstraint() {
    Pod.LabelConstraint.Builder labelConstraint = Pod.LabelConstraint.newBuilder();
    labelConstraint.
            setKindValue(Constants.POD_CONSTRAINT_LABEL_CONSTRAINT_KIND_VALUE).
            setConditionValue(Constants.POD_CONSTRAINT_LABEL_CONSTRAINT_CONDITION_VALUE).
            setRequirement(Constants.POD_CONSTRAINT_LABEL_CONSTRAINT_REQUIREMENT).
            setLabel(getLabel());
    return labelConstraint.build();
  }

  /**
   * Container Spec
   * @return
   */
  public static Pod.ContainerSpec getContainerSpec(Configuration conf) throws IOException {
    Pod.ContainerSpec.Builder containerSpecBuilder = Pod.ContainerSpec.newBuilder();

    String imageName = conf.get(YarnConfiguration.PELOTON_DOCKER_IMAGE_NAME);
    String imageTag = conf.get(YarnConfiguration.PELOTON_DOCKER_IMAGE_TAG);
    if (imageName == null || imageTag == null) {
      throw new IOException("Missing peloton docker image");
    }
    List<VolumeSpec> volumeSpecs = VolumeSpec.getAllVolumes();
    containerSpecBuilder.
            setName(Constants.CONTAINER_INFO_NAME).
            setImage(imageName + imageTag).
            setEntrypoint(getCommandSpec()).
            setResource(getResourceSpec());

    // add all volumes
    for (int i = 0; i < volumeSpecs.size(); i++) {
      containerSpecBuilder.addVolumeMounts(getVolumeMount(volumeSpecs.get(i)));
    }
    // add all environments
    for (String key : ENVIRONMENT_PARAM_MAP.keySet()) {
      containerSpecBuilder.addEnvironment(getEnvironmentParam(key));
    }

    return containerSpecBuilder.build();
  }

  /**
   * Get command spec
   * @return
   */
  public static Pod.CommandSpec getCommandSpec() {
    Pod.CommandSpec.Builder commandSpec = Pod.CommandSpec.newBuilder();
    commandSpec.
            setValue(Constants.POD_SPEC_SHELL).
            addArguments("-c").
            addArguments(Constants.POD_SPEC_SHELL_VALUE);
    return commandSpec.build();
  }
  /**
   * Resource spec
   * @return
   */
  public static Pod.ResourceSpec getResourceSpec() {
    Pod.ResourceSpec.Builder resourceSpec = Pod.ResourceSpec.newBuilder();
    resourceSpec.setCpuLimit(Constants.POD_SPEC_CPU_LIMIT).
            setMemLimitMb(Constants.POD_SPEC_MEM_LIMIT).
            setDiskLimitMb(Constants.POD_SPEC_DISK_LIMIT);
    return resourceSpec.build();
  }

  /**
   * Get volume mount
   * @param volumeSpec
   * @return
   */
  public static Pod.VolumeMount getVolumeMount(VolumeSpec volumeSpec) {
    Pod.VolumeMount.Builder volumenMountBuilder = Pod.VolumeMount.newBuilder();
    volumenMountBuilder.
            setName(volumeSpec.getVolumeName()).
            setMountPath(volumeSpec.getVolumeMount()).
            setReadOnly(volumeSpec.getVolumeMountScope());
    return volumenMountBuilder.build();
  }

  /**
   * Look up Environment param with key
   * @param key
   * @return
   */
  public static Pod.Environment getEnvironmentParam(String key) {
    Pod.Environment.Builder paramBuilder = Pod.Environment.newBuilder();
    paramBuilder.
            setName(key).
            setValue(ENVIRONMENT_PARAM_MAP.get(key));
    return paramBuilder.build();
  }

  /** Look up respool **/
  public static String lookupPool(ResourceManagerGrpc.ResourceManagerBlockingStub resourceManager,
    String resourcePoolPath) {
    Respool.LookupRequest request = Respool.LookupRequest.newBuilder()
            .setPath(Respool.ResourcePoolPath.newBuilder()
                    .setValue(resourcePoolPath)
                    .build())
            .build();

    Respool.LookupResponse response = resourceManager.lookupResourcePoolID(request);
    LOG.info(String
            .format("Looked up pool successfully: name=%s, id=%s", resourcePoolPath,
                    response.getId().toString()));
    return response.getId().getValue();
  }

  /**
   * Update region from outside
   * @param value
   */
  public static void updateJobSpecRegion(String value) {
    ENVIRONMENT_PARAM_MAP.put(Constants.UBER_REGION_ENVIRON_KEY, value);
  }

  /**
   * Update zone from outside
   * @param value
   */
  public static void updateJobSpecZone(String value) {
    ENVIRONMENT_PARAM_MAP.put(Constants.UBER_ZONE_ENVIRON_KEY, value);
  }

  /**
   * Update odin instance from outside
   * @param value
   */
  public static void updateJobSpecOdinInstance(String value) {
    ENVIRONMENT_PARAM_MAP.put(Constants.ODIN_INSTANCE, value);
  }
}
