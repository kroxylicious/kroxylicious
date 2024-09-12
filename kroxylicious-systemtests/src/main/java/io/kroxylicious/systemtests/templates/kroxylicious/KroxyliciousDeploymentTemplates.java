/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.templates.ContainerTemplates;

/**
 * The type Kroxylicious deployment templates.
 */
public class KroxyliciousDeploymentTemplates {

    private static final Map<String, String> kroxyLabelSelector = Map.of("app", "kroxylicious");
    private static final String CONFIG_VOLUME_NAME = "config-volume";

    /**
     * Default kroxylicious deployment deployment builder.
     *
     * @param namespaceName the namespace name
     * @param containerImage the container image
     * @param replicas the replicas
     * @return the deployment builder
     */
    public static DeploymentBuilder defaultKroxyDeployment(String namespaceName, String containerImage, int replicas) {
        return new DeploymentBuilder()
                                      .withApiVersion("apps/v1")
                                      .withKind(Constants.DEPLOYMENT)
                                      .withNewMetadata()
                                      .withName(Constants.KROXY_DEPLOYMENT_NAME)
                                      .withNamespace(namespaceName)
                                      .addToLabels(kroxyLabelSelector)
                                      .endMetadata()
                                      .withNewSpec()
                                      .withReplicas(replicas)
                                      .withNewSelector()
                                      .addToMatchLabels(kroxyLabelSelector)
                                      .endSelector()
                                      .withNewTemplate()
                                      .withNewMetadata()
                                      .withLabels(kroxyLabelSelector)
                                      .endMetadata()
                                      .withNewSpec()
                                      .withContainers(
                                              ContainerTemplates.baseImageBuilder("kroxylicious", containerImage)
                                                                .withArgs("--config", "/opt/kroxylicious/config/config.yaml")
                                                                .withPorts(getPlainContainerPortList())
                                                                .withVolumeMounts(getPlainVolumeMountList())
                                                                .build()
                                      )
                                      .withImagePullSecrets(
                                              new LocalObjectReferenceBuilder()
                                                                               .withName("regcred")
                                                                               .build()
                                      )
                                      .addNewVolume()
                                      .withName(CONFIG_VOLUME_NAME)
                                      .withNewConfigMap()
                                      .withName(Constants.KROXY_CONFIG_NAME)
                                      .endConfigMap()
                                      .endVolume()
                                      .endSpec()
                                      .endTemplate()
                                      .endSpec();
    }

    private static List<VolumeMount> getPlainVolumeMountList() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(
                new VolumeMountBuilder()
                                        .withName(CONFIG_VOLUME_NAME)
                                        .withMountPath("/opt/kroxylicious/config/config.yaml")
                                        .withSubPath("config.yaml")
                                        .build()
        );
        return volumeMountList;
    }

    private static List<ContainerPort> getPlainContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort("metrics", 9190));
        portList.add(createContainerPort(9292));
        portList.add(createContainerPort(9293));
        portList.add(createContainerPort(9294));
        portList.add(createContainerPort(9295));

        return portList;
    }

    private static ContainerPort createContainerPort(int port) {
        return baseContainerPort(port).build();
    }

    private static ContainerPort createContainerPort(String name, int port) {
        return baseContainerPort(port)
                                      .withName(name)
                                      .build();
    }

    private static ContainerPortBuilder baseContainerPort(int port) {
        return new ContainerPortBuilder()
                                         .withContainerPort(port);
    }
}
