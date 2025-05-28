/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.DefaultManagedWorkflowAndDependentResourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.KROXYLICIOUS_IMAGE_ENV_VAR;
import static io.kroxylicious.kubernetes.operator.resolver.DependencyResolver.EMPTY_RESOLUTION_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProxyDeploymentTest {

    private static final String PROXY_NAME = "kproxy";

    private KafkaProxy kafkaProxy;
    private Context<KafkaProxy> kubernetesContext;
    private VirtualKafkaCluster virtualKafkaCluster;
    private KafkaService kafkaService;
    private DefaultManagedWorkflowAndDependentResourceContext<KafkaProxy> resourceContext;

    @BeforeEach
    void setUp() {
        PodTemplateSpec podTemplate = new PodTemplateSpecBuilder().withNewMetadata().addToLabels("c", "d").addToLabels("a", "b").endMetadata().build();
        kafkaProxy = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).withUid(UUID.randomUUID().toString()).endMetadata()
                .withNewSpec().withPodTemplate(podTemplate).endSpec().build();
        kubernetesContext = setupContext();
    }

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR)
    void operandImageDefault() {
        assertThat(ProxyDeploymentDependentResource.getOperandImage())
                .matches("^quay.io/kroxylicious/kroxylicious:.*");
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR, value = "quay.io/myorg/kroxylicious:1")
    void operandImageOverrideFromEnvironment() {
        assertThat(ProxyDeploymentDependentResource.getOperandImage())
                .isEqualTo("quay.io/myorg/kroxylicious:1");
    }

    // labels don't technically need to be ordered, but deterministic output reduces noise when comparing output YAML
    @Test
    void podLabelsDeterministicallyOrdered() {
        // Given
        LinkedHashMap<String, String> expected = expectedLabels();
        // When
        Map<String, String> labels = ProxyDeploymentDependentResource.podLabels(kafkaProxy);

        // Then
        assertThat(labels).containsExactlyEntriesOf(expected);
    }

    @Test
    void shouldSpecifySecCompProfile() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        Deployment actual = proxyDeploymentDependentResource.desired(kafkaProxy, kubernetesContext);

        // Then
        assertThat(actual.getSpec().getTemplate().getSpec().getSecurityContext().getSeccompProfile().getType()).isEqualTo("RuntimeDefault");
    }

    @Test
    void shouldAddReferentChecksumAnnotation() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        Deployment actual = proxyDeploymentDependentResource.desired(kafkaProxy, kubernetesContext);

        // Then
        OperatorAssertions.assertThat(actual.getSpec().getTemplate().getMetadata())
                .hasAnnotationSatisfying(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION,
                        value -> assertThat(value).isNotBlank());
    }

    @Test
    void shouldAddReferentChecksumOfProxy() {
        // Given
        var proxyModel = new ProxyModel(EMPTY_RESOLUTION_RESULT, new ProxyNetworkingModel(List.of(), List.of()), List.of());
        configureProxyModel(proxyModel);
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        Deployment actual = proxyDeploymentDependentResource.desired(kafkaProxy, kubernetesContext);

        // Then
        OperatorAssertions.assertThat(actual.getSpec().getTemplate().getMetadata())
                .hasAnnotationSatisfying(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION, value -> assertThat(value).isNotBlank());

    }

    @NonNull
    @SuppressWarnings("unchecked")
    private Context<KafkaProxy> setupContext() {
        virtualKafkaCluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("vkc").withUid(UUID.randomUUID().toString())
                .withGeneration(3L).endMetadata().build();
        kafkaService = new KafkaServiceBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata().build();

        var proxyModel = new ProxyModel(EMPTY_RESOLUTION_RESULT, new ProxyNetworkingModel(List.of(), List.of()),
                List.of(clusterResolutionResultFor(virtualKafkaCluster)));

        Context<KafkaProxy> context = mock(Context.class);
        resourceContext = new DefaultManagedWorkflowAndDependentResourceContext<>(null, kafkaProxy, context);
        configureProxyModel(proxyModel);
        when(context.managedWorkflowAndDependentResourceContext()).thenReturn(resourceContext);
        return context;
    }

    private void configureProxyModel(ProxyModel proxyModel) {
        resourceContext.put(KafkaProxyContext.KEY_CTX,
                new KafkaProxyContext(new VirtualKafkaClusterStatusFactory(Clock.systemUTC()), proxyModel, Optional.empty(), List.of(), List.of()));
    }

    @NonNull
    private static LinkedHashMap<String, String> expectedLabels() {
        LinkedHashMap<String, String> expected = new LinkedHashMap<>();
        expected.put("app", "kroxylicious");
        expected.put("c", "d");
        expected.put("a", "b");
        expected.put("app.kubernetes.io/part-of", "kafka");
        expected.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        expected.put("app.kubernetes.io/name", "kroxylicious-proxy");
        expected.put("app.kubernetes.io/instance", PROXY_NAME);
        expected.put("app.kubernetes.io/component", "proxy");
        return expected;
    }

    @NonNull
    private ClusterResolutionResult clusterResolutionResultFor(VirtualKafkaCluster virtualKafkaCluster) {
        return new ClusterResolutionResult(virtualKafkaCluster,
                buildResolutionResultFromCluster(kafkaProxy),
                List.of(),
                buildResolutionResultFromCluster(kafkaService),
                List.of());
    }

    @NonNull
    private <T extends HasMetadata> ResolutionResult<T> buildResolutionResultFromCluster(T referent) {
        return new ResolutionResult<>(ResourcesUtil.toLocalRef(virtualKafkaCluster), ResourcesUtil.toLocalRef(referent), referent);
    }
}
