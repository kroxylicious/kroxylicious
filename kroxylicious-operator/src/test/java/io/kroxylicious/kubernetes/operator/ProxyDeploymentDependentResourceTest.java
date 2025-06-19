/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.DefaultManagedWorkflowAndDependentResourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.resolver.DependencyResolver.EMPTY_RESOLUTION_RESULT;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProxyDeploymentDependentResourceTest {

    private static final String PROXY_NAME = "RandomProxy";
    private KafkaProxy kafkaProxy;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private Context<KafkaProxy> kubernetesContext;

    @Mock
    private MetadataChecksumGenerator metadataChecksumGenerator;

    private ProxyModel proxyModel;

    private VirtualKafkaCluster virtualKafkaCluster;
    private KafkaService kafkaService;

    @BeforeEach
    void setUp() {
        kafkaProxy = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata()
                .withNewSpec().endSpec().build();

        kafkaService = new KafkaServiceBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata().build();
        virtualKafkaCluster = new VirtualKafkaClusterBuilder().build();

        proxyModel = new ProxyModel(EMPTY_RESOLUTION_RESULT, new ProxyNetworkingModel(List.of()), List.of(clusterResolutionResultFor(virtualKafkaCluster)));

        var resourceContext = new DefaultManagedWorkflowAndDependentResourceContext<>(null, kafkaProxy, kubernetesContext);
        resourceContext.put(Crc32ChecksumGenerator.CHECKSUM_CONTEXT_KEY, metadataChecksumGenerator);
        when(kubernetesContext.managedWorkflowAndDependentResourceContext()).thenReturn(resourceContext);
    }

    @Test
    void shouldNotIncludeKafkaProxyInChecksum() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext, proxyModel);

        // Then
        verify(metadataChecksumGenerator, times(0)).appendMetadata(kafkaProxy);
    }

    @Test
    void shouldIncludeResolvedVirtualClusterInChecksum() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext, proxyModel);

        // Then
        verify(metadataChecksumGenerator).appendMetadata(virtualKafkaCluster);
    }

    @Test
    void shouldIncludeMultipleResolvedVirtualClusterInChecksum() {
        // Given
        var virtualKafkaClusterB = new VirtualKafkaClusterBuilder().build();
        List<ClusterResolutionResult> clusterResolutionResults = List.of(
                clusterResolutionResultFor(virtualKafkaCluster),
                clusterResolutionResultFor(virtualKafkaClusterB));
        proxyModel = new ProxyModel(EMPTY_RESOLUTION_RESULT, new ProxyNetworkingModel(List.of()), clusterResolutionResults);
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext, proxyModel);

        // Then
        verify(metadataChecksumGenerator, times(clusterResolutionResults.size())).appendMetadata(ArgumentMatchers.any(VirtualKafkaCluster.class));
    }

    @Test
    void shouldEncodeChecksum() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext, proxyModel);

        // Then
        verify(metadataChecksumGenerator).encode();
        // We can't assert anything about the checksum here as the generator is mocked, we just want to verify the generator is used correctly
    }

    @Test
    void shouldDefaultToNullResourceRequest() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        ResourceRequirements actualRequirements = proxyDeploymentDependentResource.proxyContainerResources(kafkaProxy);

        // Then
        Assertions.assertThat(actualRequirements).isNull();
    }

    @Test
    void shouldUseSuppliedResourcesResourceRequest() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();
        Map<String, Quantity> resources = Map.of("cpu", Quantity.parse("1000m"), "memory", Quantity.parse("1024Mi"));
        ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder()
                .withRequests(resources)
                .withLimits(resources)
                .build();
        KafkaProxy proxyWithLimits = kafkaProxy.edit().editOrNewSpec().editOrNewInfrastructure().editOrNewProxyContainer().withResources(resourceRequirements)
                .endProxyContainer()
                .endInfrastructure().endSpec().build();

        // When
        ResourceRequirements actualRequirements = proxyDeploymentDependentResource.proxyContainerResources(proxyWithLimits);

        // Then
        Assertions.assertThat(actualRequirements).isNotNull().isEqualTo(resourceRequirements);
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
