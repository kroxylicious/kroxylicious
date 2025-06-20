/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.management.UnsupportedHttpMethodFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(MockitoExtension.class)
class OperatorMainTest {

    KubernetesClient kubeClient;
    KubernetesMockServer mockServer;

    private OperatorMain operatorMain;

    @Mock
    HttpServer managementServer;

    @Mock
    HttpContext rootHttpContext;
    ArgumentCaptor<HttpHandler> rootCaptor = ArgumentCaptor.forClass(HttpHandler.class);

    @Mock
    HttpContext metricsHttpContext;

    @Mock
    HttpContext livezHttpContext;
    ArgumentCaptor<HttpHandler> livezCaptor = ArgumentCaptor.forClass(HttpHandler.class);

    @BeforeEach
    void setUp() {
        expectApiResources();
        when(managementServer.createContext(eq("/"), rootCaptor.capture())).thenReturn(rootHttpContext);
        when(managementServer.createContext(eq(OperatorMain.HTTP_PATH_METRICS), any(HttpHandler.class))).thenReturn(metricsHttpContext);
        when(managementServer.createContext(eq(OperatorMain.HTTP_PATH_LIVEZ), livezCaptor.capture())).thenReturn(livezHttpContext);
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
        assertThat(Metrics.globalRegistry.get("operator.sdk.reconciliations.executions.kafkaproxyreconciler").meter().getId()).isNotNull();
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
        verify(managementServer).createContext(eq(OperatorMain.HTTP_PATH_METRICS), any(HttpHandler.class));
    }

    @Test
    void shouldRegisterLivezWithManagementServer() {
        // Given

        // When
        operatorMain.start();

        // Then
        verify(managementServer).createContext(eq(OperatorMain.HTTP_PATH_LIVEZ), any(HttpHandler.class));
    }

    @Test
    void shouldRegisterUnsupportedMethodsHandlerWithManagementServer() {
        // Given
        final ArrayList<Filter> filters = new ArrayList<>();
        when(rootHttpContext.getFilters()).thenReturn(filters);

        // When
        operatorMain.start();

        // Then
        verify(managementServer).createContext(eq("/"), any(HttpHandler.class));
        assertThat(filters)
                .singleElement()
                .isInstanceOf(UnsupportedHttpMethodFilter.class);

    }

    @Test
    void shouldRespondWith404ForRequestsManagementServer() throws IOException {
        shouldRespondWithStatusCode(rootCaptor, 404);
    }

    @Test
    void shouldRespondWith200ForRequestsLivez() throws IOException {
        // Given
        shouldRespondWithStatusCode(livezCaptor, 200);
    }

    private void shouldRespondWithStatusCode(ArgumentCaptor<HttpHandler> captor,
                                             int statusCode)
            throws IOException {
        // Given
        operatorMain.start();
        final HttpExchange httpExchange = mock(HttpExchange.class);

        // When
        captor.getValue().handle(httpExchange);

        // Then
        verify(httpExchange).sendResponseHeaders(statusCode, -1);
    }

    private void expectApiResources() {
        expectCrd(KafkaProtocolFilter.class);
        expectCrd(KafkaProxy.class);
        expectCrd(VirtualKafkaCluster.class);
        expectCrd(KafkaService.class);
    }

    private void expectCrd(Class<? extends CustomResource<?, ?>> crdClass) {
        final CustomResourceDefinition resourceDefinition = CustomResourceDefinitionContext.v1CRDFromCustomResourceType(crdClass)
                .build();
        mockServer.expectCustomResource(CustomResourceDefinitionContext.fromCrd(resourceDefinition));
    }
}
