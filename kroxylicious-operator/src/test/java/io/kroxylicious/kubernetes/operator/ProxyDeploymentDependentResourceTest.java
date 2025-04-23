/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.DefaultManagedWorkflowAndDependentResourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProxyDeploymentDependentResourceTest {

    private static final String PROXY_NAME = "RandomProxy";
    private static final String PROXY_INGRESS_CHECKSUM = "12345";
    private static final String PROXY_INGRESS_UID = UUID.randomUUID().toString();
    private static final long PROXY_INGRESS_GENERATION = 10L;
    private KafkaProxy kafkaProxy;

    @Mock
    private Context<KafkaProxy> kubernetesContext;

    @Mock
    private MetadataChecksumGenerator metadataChecksumGenerator;

    private KafkaProxyIngress kafkaProxyIngress;

    @BeforeEach
    void setUp() {
        PodTemplateSpec podTemplate = new PodTemplateSpecBuilder().withNewMetadata().addToLabels("c", "d").addToLabels("a", "b").endMetadata().build();
        kafkaProxy = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata()
                .withNewSpec().withPodTemplate(podTemplate).endSpec().build();

        kafkaProxyIngress = new KafkaProxyIngressBuilder().withNewMetadata().withUid(PROXY_INGRESS_UID).withName("Ingress").withGeneration(PROXY_INGRESS_GENERATION)
                .withAnnotations(Map.of(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION, PROXY_INGRESS_CHECKSUM)).endMetadata().build();
        var resourceContext = new DefaultManagedWorkflowAndDependentResourceContext<>(null, null, kubernetesContext);
        resourceContext.put(Crc32ChecksumGenerator.CHECKSUM_CONTEXT_KEY, metadataChecksumGenerator);
        when(kubernetesContext.managedWorkflowAndDependentResourceContext()).thenReturn(resourceContext);
        when(kubernetesContext.getSecondaryResource(KafkaProxyIngress.class)).thenReturn(Optional.of(kafkaProxyIngress));
    }

    @Test
    void shouldNotIncludeKafkaProxyIngressWithoutChecksum() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext);

        // Then
        verify(metadataChecksumGenerator, times(0)).appendString(PROXY_INGRESS_CHECKSUM);
    }

    @Test
    void shouldSkipMissingKafkaProxyIngress() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();
        when(kubernetesContext.getSecondaryResource(KafkaProxyIngress.class)).thenReturn(Optional.empty());

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext);

        // Then
        verify(metadataChecksumGenerator, times(0)).appendString(anyString());
    }

    @Test
    void shouldIncludeKafkaProxyIngressUidAndGeneration() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext);

        // Then
        verify(metadataChecksumGenerator).appendMetadata(kafkaProxyIngress);
        // We can't assert anything about the checksum here as the generator is mocked, we just want to verify the generator is used correctly
    }

    @Test
    void shouldEncodeChecksum() {
        // Given
        ProxyDeploymentDependentResource proxyDeploymentDependentResource = new ProxyDeploymentDependentResource();

        // When
        proxyDeploymentDependentResource.checksumFor(kafkaProxy, kubernetesContext);

        // Then
        verify(metadataChecksumGenerator).encode();
        // We can't assert anything about the checksum here as the generator is mocked, we just want to verify the generator is used correctly
    }
}