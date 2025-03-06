/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@EnableKubernetesMockClient(crud = true)
class OperatorMainTest {

    KubernetesClient kubeClient;
    KubernetesMockServer mockServer;

    private OperatorMain operatorMain;

    @Mock
    CompositeMeterRegistry globalRegistry;

    @BeforeEach
    void setUp() {
        expectApiResources();
        operatorMain = new OperatorMain(() -> globalRegistry, kubeClient);
    }

    @Test
    void shouldRegisterPrometheusMeterRegistry() {
        // Given

        // When
        operatorMain.start();

        // Then
        verify(globalRegistry).add(argThat(PrometheusMeterRegistry.class::isInstance));
        mockServer.shutdown();
    }

    @Test
    void shouldRegisterOperatorMetrics() {
        // Given
        final KafkaProxyBuilder proxyBuilder = new KafkaProxyBuilder().withKind("KafkaProxy").withNewMetadata().withName("myCoolProxy").endMetadata();
        operatorMain.start();

        // When
        kubeClient.resource(proxyBuilder.build()).create();

        // Then
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .ignoreException(MeterNotFoundException.class)
                .untilAsserted(() -> assertThat(operatorMain.getRegistry().get("operator.sdk.events.received").meter().getId()).isNotNull());

    }

    @Test
    void shouldRegisterMetricsForProxyReconciler() {
        // Given

        // When
        operatorMain.start();

        // Then
        assertThat(operatorMain.getRegistry().get("operator.sdk.reconciliations.executions.proxyreconciler").meter().getId()).isNotNull();
    }

    private void expectApiResources() {
        final CustomResourceDefinition kafkaProtocolFilterCrd = CustomResourceDefinitionContext.v1CRDFromCustomResourceType(KafkaProtocolFilter.class).withNewMetadata()
                .endMetadata()
                .build();
        mockServer.expectCustomResource(CustomResourceDefinitionContext.fromCrd(kafkaProtocolFilterCrd));
        final CustomResourceDefinition kafkaProxyCrd = CustomResourceDefinitionContext.v1CRDFromCustomResourceType(KafkaProtocolFilter.class).withNewMetadata()
                .endMetadata()
                .build();
        mockServer.expectCustomResource(CustomResourceDefinitionContext.fromCrd(kafkaProxyCrd));
    }
}
