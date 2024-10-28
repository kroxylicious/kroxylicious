/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Map;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * The Kube {@code Deployment} for the proxy
 */
@KubernetesDependent
public class ProxyDeployment
        extends CRUDKubernetesDependentResource<Deployment, KafkaProxy> {

    public static final String CONFIG_VOLUME = "config-volume";
    public static final String CONFIG_PATH_IN_CONTAINER = "/opt/kroxylicious/config/config.yaml";
    public static final Map<String, String> APP_KROXY = Map.of("app", "kroxylicious");

    public ProxyDeployment() {
        super(Deployment.class);
    }

    static String deploymentName(KafkaProxy primary) {
        return primary.getMetadata().getName();
    }

    @Override
    protected Deployment desired(KafkaProxy primary,
                                 Context<KafkaProxy> context) {
        return new DeploymentBuilder()
                .editOrNewMetadata()
                .withName(deploymentName(primary))
                .withNamespace(primary.getMetadata().getNamespace())
                .withLabels(deploymentLabels())
                .endMetadata()
                .editOrNewSpec()
                .withReplicas(1)
                .editOrNewSelector()
                .withMatchLabels(deploymentSelector())
                .endSelector()
                .withTemplate(podTemplate(primary))
                .endSpec()
                .build();
    }

    private static Map<String, String> deploymentSelector() {
        return APP_KROXY;
    }

    private static Map<String, String> deploymentLabels() {
        return APP_KROXY;
    }

    static Map<String, String> podLabels() {
        return APP_KROXY;
    }

    private PodTemplateSpec podTemplate(KafkaProxy primary) {
        return new PodTemplateSpecBuilder()
                .editOrNewMetadata()
                .withLabels(podLabels())
                .endMetadata()
                .editOrNewSpec()
                .withContainers(proxyContainer(primary, CONFIG_PATH_IN_CONTAINER))
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
                .withName("proxy")
                .withImage("quay.io/kroxylicious/kroxylicious:0.9.0-SNAPSHOT")
                .withArgs("--config", configPathInContainer);
        // volume mount
        containerBuilder
                .addNewVolumeMount()
                .withName(CONFIG_VOLUME)
                .withMountPath(configPathInContainer)
                .withSubPath(ProxyConfigSecret.configYamlKey())
                .endVolumeMount();
        // metrics port
        containerBuilder.addNewPort()
                .withContainerPort(ProxyService.metricsPort())
                .withName("metrics")
                .endPort();
        // broker ports
        for (int portNum : ProxyService.brokerPorts()) {
            containerBuilder.addNewPort()
                    .withContainerPort(portNum)
                    .endPort();
        }
        return containerBuilder.build();
    }
}
