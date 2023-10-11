/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Kroxy deployment templates.
 */
public class KroxyDeploymentTemplates {

    private static Map<String, String> getKroxyLabelsSelector() {
        return new HashMap<>() {
            {
                put("app", "kroxylicious");
            }
        };
    }

    /**
     * Default kroxy deployment deployment builder.
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
                .withName("kroxylicious-proxy")
                .withNamespace(namespaceName)
                .addToLabels(getKroxyLabelsSelector())
                .endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withNewSelector()
                .addToMatchLabels(getKroxyLabelsSelector())
                .endSelector()
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(getKroxyLabelsSelector())
                .endMetadata()
                .withNewSpec()
                .withContainers(new ContainerBuilder()
                        .withName("kroxylicious")
                        .withImage(containerImage) // "quay.io/kroxylicious/kroxylicious-developer:0.3.0-SNAPSHOT")
                        .withImagePullPolicy("Always")
                        .withArgs("--config", "/opt/kroxylicious/config/config.yaml")
                        .withPorts(getPlainContainerPortList())
                        .withVolumeMounts(getPlainVolumeMountList())
                        .build())
                .addNewVolume()
                .withName("config-volume")
                .withNewConfigMap()
                .withName("kroxylicious-config")
                .endConfigMap()
                .endVolume()
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    private static List<VolumeMount> getPlainVolumeMountList() {
        List<VolumeMount> volumeMountList = new ArrayList<>();
        volumeMountList.add(new VolumeMountBuilder()
                .withName("config-volume")
                .withMountPath("/opt/kroxylicious/config/config.yaml")
                .withSubPath("config.yaml")
                .build());
        return volumeMountList;
    }

    private static List<ContainerPort> getPlainContainerPortList() {
        List<ContainerPort> portList = new ArrayList<>();
        portList.add(createContainerPort(9190));
        portList.add(createContainerPort(9292));
        portList.add(createContainerPort(9293));
        portList.add(createContainerPort(9294));
        portList.add(createContainerPort(9295));

        return portList;
    }

    private static ContainerPort createContainerPort(int port) {
        return new ContainerPortBuilder()
                .withContainerPort(port)
                .build();
    }
}
