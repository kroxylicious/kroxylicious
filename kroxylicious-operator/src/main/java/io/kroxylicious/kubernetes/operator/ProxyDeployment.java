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

    /**
     * @return The {@code metadata.name} of the desired {@code Deployment}.
     */
    static String deploymentName(KafkaProxy primary) {
        return primary.getMetadata().getName();
    }

    @Override
    protected Deployment desired(KafkaProxy primary,
                                 Context<KafkaProxy> context) {
        // formatter=off
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
        // formatter=off
        return new PodTemplateSpecBuilder()
                .editOrNewMetadata()
                    .withLabels(podLabels())
                .endMetadata()
                .editOrNewSpec()
                    .withContainers(proxyContainer(primary))
                    .addNewVolume()
                        .withName(CONFIG_VOLUME)
                        .withNewSecret()
                            .withSecretName(ProxyConfigSecret.secretName(primary))
                        .endSecret()
                    .endVolume()
                .endSpec()
                .build();
    }

    private static Container proxyContainer(KafkaProxy primary) {
        // formatter=off
        var containerBuilder = new ContainerBuilder()
                .withName("proxy")
                .withImage("quay.io/kroxylicious/kroxylicious:0.9.0-SNAPSHOT")
                .withArgs("--config", ProxyDeployment.CONFIG_PATH_IN_CONTAINER)
                // volume mount
                .addNewVolumeMount()
                    .withName(CONFIG_VOLUME)
                    .withMountPath(ProxyDeployment.CONFIG_PATH_IN_CONTAINER)
                    .withSubPath(ProxyConfigSecret.CONFIG_YAML_KEY)
                .endVolumeMount()
                // metrics port
                .addNewPort()
                    .withContainerPort(MetricsService.metricsPort())
                    .withName("metrics")
                .endPort();
        // broker ports
        for (var cluster : ClustersUtil.distinctClusters(primary)) {
            for (var portEntry : ClusterService.clusterPorts(primary, cluster).entrySet()) {
                containerBuilder.addNewPort()
                        .withContainerPort(portEntry.getKey())
                        // .withName(portEntry.getValue())
                        .endPort();
            }
        }
        return containerBuilder.build();
    }
}
