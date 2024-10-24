/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.operator;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.crapi.v1alpha1.KafkaProxy;

/**
 * The Kube {@code Deployment} for the proxy
 */
@KubernetesDependent
public class ProxyDeployment
        extends CRUDKubernetesDependentResource<Deployment, KafkaProxy> {

    public static final String CONFIG_VOLUME = "config-volume";

    public ProxyDeployment() {
        super(Deployment.class);
    }

    static String deploymentName(KafkaProxy primary) {
        return primary.getMetadata().getName();
    }

    @Override
    protected Deployment desired(KafkaProxy primary,
                                 Context<KafkaProxy> context) {
        String configPathInContainer = "/opt/kroxylicious/config/config.yaml";
        return new DeploymentBuilder()
                .editOrNewMetadata()
                .withName(deploymentName(primary))
                .withNamespace(primary.getMetadata().getNamespace())
                .addToLabels("app", "kroxylicious")
                .endMetadata()
                .editOrNewSpec()
                .withReplicas(1)
                .editOrNewSelector()
                .addToMatchLabels("app", "kroxylicious")
                .endSelector()
                .withTemplate(podTemplate(primary, configPathInContainer))
                .endSpec()
                .build();
    }

    private PodTemplateSpec podTemplate(KafkaProxy primary, String configPathInContainer) {
        return new PodTemplateSpecBuilder()
                .editOrNewMetadata()
                .addToLabels("app", "kroxylicious")
                .endMetadata()
                .editOrNewSpec()
                .withContainers(proxyContainer(primary, configPathInContainer))
                .addNewVolume()
                .withName(CONFIG_VOLUME)
                .withNewSecret()
                .withSecretName(ProxyConfigSecret.secretName(primary))
                .endSecret()
                .endVolume()
                .endSpec()
                .build();
    }

    private static Container proxyContainer(KafkaProxy primary, String configPathInContainer) {
        var containerBuilder = new ContainerBuilder()
                .withName("kroxylicious")
                .withImage("quay.io/kroxylicious/kroxylicious:0.9.0-SNAPSHOT")
                .withArgs("--config", configPathInContainer);
        // volume mount
        containerBuilder
                .addNewVolumeMount()
                .withName(CONFIG_VOLUME)
                .withMountPath(configPathInContainer)
                .withSubPath(ProxyConfigSecret.configYamlKey(primary))
                .endVolumeMount();
        // metrics port
        containerBuilder.addNewPort()
                .withContainerPort(ProxyService.metricsPort(primary))
                .withName("metrics")
                .endPort();
        // broker ports
        for (int portNum : ProxyService.brokerPorts(primary)) {
            containerBuilder.addNewPort()
                    .withContainerPort(portNum)
                    .endPort();
        }
        return containerBuilder.build();
    }
}
