/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;

/**
 * Generates the Kube {@code Deployment} for the proxy
 */
@KubernetesDependent
public class ProxyDeployment
        extends CRUDKubernetesDependentResource<Deployment, KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyDeployment.class);
    public static final String CONFIG_VOLUME = "config-volume";
    public static final String CONFIG_PATH_IN_CONTAINER = "/opt/kroxylicious/config/" + ProxyConfigSecret.CONFIG_YAML_KEY;
    public static final Map<String, String> APP_KROXY = Map.of("app", "kroxylicious");
    public static final int METRICS_PORT = 9190;
    private final String kroxyliciousImage = getOperandImage();
    static final String KROXYLICIOUS_IMAGE_ENV_VAR = "KROXYLICIOUS_IMAGE";

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
        // @formatter:off
        return new DeploymentBuilder()
                .editOrNewMetadata()
                    .withName(deploymentName(primary))
                    .withNamespace(primary.getMetadata().getNamespace())
                    .addNewOwnerReferenceLike(ResourcesUtil.ownerReferenceTo(primary)).endOwnerReference()
                    .addToLabels(APP_KROXY)
                    .addToLabels(standardLabels(primary))
                .endMetadata()
                .editOrNewSpec()
                    .withReplicas(1)
                    .editOrNewSelector()
                        .withMatchLabels(deploymentSelector())
                    .endSelector()
                    .withTemplate(podTemplate(primary, context))
                .endSpec()
                .build();
        // @formatter:on
    }

    private static Map<String, String> deploymentSelector() {
        return APP_KROXY;
    }

    static Map<String, String> podLabels() {
        return APP_KROXY;
    }

    private PodTemplateSpec podTemplate(KafkaProxy primary,
                                        Context<KafkaProxy> context) {
        Map<String, String> labelsFromSpec = Optional.ofNullable(primary.getSpec()).map(KafkaProxySpec::getPodTemplate)
                .map(PodTemplateSpec::getMetadata)
                .map(ObjectMeta::getLabels)
                .orElse(Map.of());
        // @formatter:off
        return new PodTemplateSpecBuilder()
                .editOrNewMetadata()
                    .addToLabels(labelsFromSpec)
                    .addToLabels(podLabels())
                .endMetadata()
                .editOrNewSpec()
                    .withContainers(proxyContainer( context))
                    .addNewVolume()
                        .withName(CONFIG_VOLUME)
                        .withNewSecret()
                            .withSecretName(ProxyConfigSecret.secretName(primary))
                        .endSecret()
                    .endVolume()
                .endSpec()
                .build();
        // @formatter:on
    }

    private Container proxyContainer(Context<KafkaProxy> context) {
        // @formatter:off
        var containerBuilder = new ContainerBuilder()
                .withName("proxy")
                .withImage(kroxyliciousImage)
                .withArgs("--config", ProxyDeployment.CONFIG_PATH_IN_CONTAINER)
                // volume mount
                .addNewVolumeMount()
                    .withName(CONFIG_VOLUME)
                    .withMountPath(ProxyDeployment.CONFIG_PATH_IN_CONTAINER)
                    .withSubPath(ProxyConfigSecret.CONFIG_YAML_KEY)
                .endVolumeMount()
                // metrics port
                .addNewPort()
                    .withContainerPort(METRICS_PORT)
                    .withName("metrics")
                .endPort();
        // broker ports
        ResourcesUtil.clustersInNameOrder(context).forEach(virtualKafkaCluster -> {
            if (!SharedKafkaProxyContext.isBroken(context, virtualKafkaCluster)) {
                for (var portEntry : ClusterService.clusterPorts( context, virtualKafkaCluster).entrySet()) {
                    containerBuilder.addNewPort()
                            .withContainerPort(portEntry.getKey())
                            .endPort();
                }
            }
        });
        return containerBuilder.build();
        // @formatter:on
    }

    @VisibleForTesting
    static String getOperandImage() {
        var envImage = System.getenv().get(KROXYLICIOUS_IMAGE_ENV_VAR);
        if (envImage != null && !envImage.isBlank()) {
            LOGGER.info("Using Kroxylicious operand image ({}) from environment variable {}", envImage, KROXYLICIOUS_IMAGE_ENV_VAR);
            return envImage;
        }
        else {
            var name = "/kroxylicious-image.properties";
            try (var is = ProxyDeployment.class.getResourceAsStream(name)) {
                if (is == null) {
                    throw new IllegalStateException("Failed to find %s on classpath".formatted(name));
                }
                var props = new Properties();
                props.load(is);
                var key = "kroxylicious-image";
                var image = props.getProperty(key);
                if (image == null || image.isEmpty()) {
                    throw new IllegalStateException("Classpath resource %s does not contain expected property %s".formatted(name, key));
                }
                LOGGER.info("Using Kroxylicious operand image ({}) from properties file {} on classpath", image, name);
                return image;
            }
            catch (IOException ioe) {
                throw new IllegalStateException("Failed to open %s on classpath".formatted(name), ioe);
            }
        }

    }
}
