/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.management.UnsupportedHttpMethodHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(MockitoExtension.class)
class OperatorMainTest {

    KubernetesClient kubeClient;
    KubernetesMockServer mockServer;

    private OperatorMain operatorMain;

    @Mock
    HttpServer managementServer;

    @BeforeEach
    void setUp() {
        expectApiResources();
        operatorMain = new OperatorMain(kubeClient, managementServer);
    }

    @AfterEach
    void tearDown() {
        if (operatorMain != null) {
            operatorMain.stop();
        }
    }

    @Test
    void shouldRegisterPrometheusMeterRegistry() {
        // Given

        // When
        operatorMain.start();

        // Then
        assertThat(Metrics.globalRegistry.getRegistries())
                .hasAtLeastOneElementOfType(PrometheusMeterRegistry.class);
    }

    @Test
    void shouldRegisterOperatorMetrics() {
        // Given
        final KafkaProxyBuilder proxyBuilder = new KafkaProxyBuilder().withKind("KafkaProxy").withNewMetadata().withName("mycoolproxy").endMetadata();
        operatorMain.start();

        // When
        kubeClient.resource(proxyBuilder.build()).create();

        // Then
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .ignoreException(MeterNotFoundException.class)
                .untilAsserted(() -> assertThat(Metrics.globalRegistry.get("operator.sdk.events.received").meter().getId()).isNotNull());

    }

    @Test
    void shouldRegisterMetricsForProxyReconciler() {
        // Given

        // When
        operatorMain.start();

        // Then
        assertThat(Metrics.globalRegistry.get("operator.sdk.reconciliations.executions.proxyreconciler").meter().getId()).isNotNull();
    }

    @Test
    void shouldStartHttpServer() {
        // Given

        // When
        operatorMain.start();

        // Then
        verify(managementServer).start();
    }

    @Test
    void shouldRegisterMetricsWithManagementServer() {
        // Given

        // When
        operatorMain.start();

        // Then
        verify(managementServer).createContext(eq("/metrics"), any(HttpHandler.class));
    }

    @Test
    void shouldRegisterUnsupportedMethodsHandlerWithManagementServer() {
        // Given

        // When
        operatorMain.start();

        // Then
        verify(managementServer).createContext(eq("/"), any(UnsupportedHttpMethodHandler.class));
    }

    private void expectApiResources() {
        expectCrd(KafkaProtocolFilter.class);
        expectCrd(KafkaProxy.class);
        expectCrd(VirtualKafkaCluster.class);
        expectCrd(KafkaClusterRef.class);
    }

    private void expectCrd(Class<? extends CustomResource<?, ?>> crdClass) {
        final CustomResourceDefinition resourceDefinition = CustomResourceDefinitionContext.v1CRDFromCustomResourceType(crdClass)
                .build();
        mockServer.expectCustomResource(CustomResourceDefinitionContext.fromCrd(resourceDefinition));
    }
}
