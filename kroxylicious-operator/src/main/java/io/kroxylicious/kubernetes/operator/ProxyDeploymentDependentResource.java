/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecFluent;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates the Kube {@code Deployment} for the proxy
 */
@KubernetesDependent
public class ProxyDeploymentDependentResource
        extends CRUDKubernetesDependentResource<Deployment, KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyDeploymentDependentResource.class);
    public static final String CONFIG_VOLUME = "config-volume";
    public static final String CONFIG_PATH_IN_CONTAINER = "/opt/kroxylicious/config/" + ProxyConfigDependentResource.CONFIG_YAML_KEY;
    public static final Map<String, String> APP_KROXY = Map.of("app", "kroxylicious");
    private static final int MANAGEMENT_PORT = 9190;
    private static final String MANAGEMENT_PORT_NAME = "management";
    public static final int PROXY_PORT_START = 9292;
    public static final int SHARED_SNI_PORT = 9291;
    private final String kroxyliciousImage = getOperandImage();
    static final String KROXYLICIOUS_IMAGE_ENV_VAR = "KROXYLICIOUS_IMAGE";

    public ProxyDeploymentDependentResource() {
        super(Deployment.class);
    }

    /**
     * @return The {@code metadata.name} of the desired {@code Deployment}.
     */
    static String deploymentName(KafkaProxy primary) {
        return ResourcesUtil.name(primary);
    }

    @Override
    protected Deployment desired(KafkaProxy primary,
                                 Context<KafkaProxy> context) {
        KafkaProxyContext kafkaProxyContext = KafkaProxyContext.proxyContext(context);
        var model = kafkaProxyContext.model();
        String checksum = checksumFor(primary, context, model);

        // @formatter:off
        return new DeploymentBuilder()
                .editOrNewMetadata()
                    .withName(deploymentName(primary))
                    .withNamespace(namespace(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                    .addToLabels(APP_KROXY)
                    .addToLabels(standardLabels(primary))
                .endMetadata()
                .editOrNewSpec()
                    .withReplicas(1)
                    .editOrNewSelector()
                    .withMatchLabels(deploymentSelector(primary))
                    .endSelector()
                    .withTemplate(podTemplate(primary, kafkaProxyContext, model.networkingModel(), model.clustersWithValidNetworking(), checksum))
                .endSpec()
                .build();
        // @formatter:on
    }

    @VisibleForTesting
    String checksumFor(KafkaProxy primary, Context<KafkaProxy> context, ProxyModel model) {
        MetadataChecksumGenerator checksumGenerator = context.managedWorkflowAndDependentResourceContext()
                .get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)
                .orElse(new Crc32ChecksumGenerator());
        checksumGenerator.appendMetadata(primary);
        model.clustersWithValidNetworking().stream().map(ClusterResolutionResult::cluster).forEach(checksumGenerator::appendMetadata);
        String encoded = checksumGenerator.encode();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Checksum: {} generated for KafkaProxy: {}", encoded, KubernetesResourceUtil.getName(primary));
        }
        return encoded;
    }

    private static Map<String, String> deploymentSelector(KafkaProxy primary) {
        return podLabels(primary);
    }

    public static Map<String, String> podLabels(KafkaProxy primary) {
        Map<String, String> labelsFromSpec = Optional.ofNullable(primary.getSpec()).map(KafkaProxySpec::getPodTemplate)
                .map(PodTemplateSpec::getMetadata)
                .map(ObjectMeta::getLabels)
                .orElse(Map.of());
        Map<String, String> result = new LinkedHashMap<>(APP_KROXY);
        result.putAll(labelsFromSpec);
        result.putAll(standardLabels(primary));
        return result;
    }

    private PodTemplateSpec podTemplate(KafkaProxy primary,
                                        KafkaProxyContext kafkaProxyContext,
                                        ProxyNetworkingModel ingressModel,
                                        List<ClusterResolutionResult> clusterResolutionResults,
                                        String checksum) {
        PodTemplateSpecFluent<PodTemplateSpecBuilder>.MetadataNested<PodTemplateSpecBuilder> metadataBuilder = new PodTemplateSpecBuilder()
                .editOrNewMetadata()
                .addToLabels(podLabels(primary));
        if (!checksum.isBlank()) {
            metadataBuilder.addToAnnotations(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION, checksum);
        }

        // @formatter:off
        return metadataBuilder
                .endMetadata()
                .editOrNewSpec()
                    .withNewSecurityContext()
                        .withRunAsNonRoot(true)
                        .withNewSeccompProfile()
                            .withType("RuntimeDefault")
                        .endSeccompProfile()
                    .endSecurityContext()
                    .withContainers(proxyContainer(kafkaProxyContext, ingressModel, clusterResolutionResults))
                    .addNewVolume()
                        .withName(CONFIG_VOLUME)
                        .withNewConfigMap()
                            .withName(ProxyConfigDependentResource.configMapName(primary))
                        .endConfigMap()
                    .endVolume()
                    .addAllToVolumes(kafkaProxyContext.volumes())
                .endSpec()
                .build();
        // @formatter:on
    }

    private Container proxyContainer(KafkaProxyContext kafkaProxyContext,
                                     ProxyNetworkingModel ingressModel,
                                     List<ClusterResolutionResult> clusterResolutionResults) {
        // @formatter:off
         var containerBuilder = new ContainerBuilder()
                .withName("proxy")
                .withNewLivenessProbe()
                    .withNewHttpGet()
                        .withPath("/livez")
                        .withPort(new IntOrString(MANAGEMENT_PORT_NAME))
                    .endHttpGet()
                    .withInitialDelaySeconds(10)
                    .withSuccessThreshold(1)
                    .withTimeoutSeconds(1)
                    .withFailureThreshold(3)
                .endLivenessProbe()
                .withImage(kroxyliciousImage)
                .withNewSecurityContext()
                    .withAllowPrivilegeEscalation(false)
                    .withNewCapabilities()
                        .addToDrop("ALL")
                    .endCapabilities()
                    .withReadOnlyRootFilesystem(true)
                .endSecurityContext()
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .withArgs("--config", ProxyDeploymentDependentResource.CONFIG_PATH_IN_CONTAINER)
                // volume mount
                .addNewVolumeMount()
                    .withName(CONFIG_VOLUME)
                    .withMountPath(ProxyDeploymentDependentResource.CONFIG_PATH_IN_CONTAINER)
                    .withSubPath(ProxyConfigDependentResource.CONFIG_YAML_KEY)
                .endVolumeMount()
                .addAllToVolumeMounts(kafkaProxyContext.mounts())
                // management port
                .addNewPort()
                    .withContainerPort(MANAGEMENT_PORT)
                    .withName(MANAGEMENT_PORT_NAME)
                .endPort();
        // @formatter:on
        // broker ports
        AtomicBoolean anyIngressRequiresSharedSniPort = new AtomicBoolean(false);
        clusterResolutionResults.forEach(resolutionResult -> {
            if (!kafkaProxyContext.isBroken(resolutionResult.cluster())) {
                ProxyNetworkingModel.ClusterNetworkingModel clusterNetworkingModel = ingressModel.clusterIngressModel(resolutionResult.cluster()).orElseThrow();
                clusterNetworkingModel.registerProxyContainerPorts(containerBuilder::addToPorts);
                anyIngressRequiresSharedSniPort.compareAndExchange(false, clusterNetworkingModel.anyIngressRequiresSharedSniPort());
            }
        });
        if (anyIngressRequiresSharedSniPort.get()) {
            containerBuilder.addNewPort().withContainerPort(SHARED_SNI_PORT).withName("shared-sni-port").endPort();
        }
        return containerBuilder.build();
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
            try (var is = ProxyDeploymentDependentResource.class.getResourceAsStream(name)) {
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
