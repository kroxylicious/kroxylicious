/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.APIResource;
import io.fabric8.kubernetes.api.model.APIResourceBuilder;
import io.fabric8.kubernetes.api.model.APIResourceList;
import io.fabric8.kubernetes.api.model.APIResourceListBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesList;
import io.fabric8.kubernetes.api.model.KubernetesListBuilder;
import io.fabric8.kubernetes.api.model.ListMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;

import lombok.SneakyThrows;

@ExtendWith(MockitoExtension.class)
@EnableKubernetesMockClient(crud = true)
class OperatorMainTest {

    private static final String START_RESOURCE_VERSION = "1000";
    KubernetesClient kubeClient;
    KubernetesMockServer mockServer;

    private OperatorMain operatorMain;

    @Mock
    CompositeMeterRegistry globalRegistry;

    @BeforeEach
    void setUp() {
        expectApiResources();

        expectWatchOn("/apis/kroxylicious.io/v1alpha1/kafkaproxies", KafkaProxy.class, "kafkaproxy", "kroxylicious.io/v1alpha1", 1000);
        expectWatchOn("/apis/kroxylicious.io/v1alpha1/virtualkafkaclusters", VirtualKafkaCluster.class, "virtualkafkacluster", "kroxylicious.io/v1alpha1", 1000);
        expectWatchOn("/apis/filter.kroxylicious.io/v1alpha1/kafkaprotocolfilters", KafkaProtocolFilter.class, "kafkaprotocolfilter", "kroxylicious.io/v1alpha1", 0);
        expectWatchOn("/apis/apps/v1/deployments", Deployment.class, "deployment", "v1", 1000);
        expectWatchOn("/api/v1/secrets", Secret.class, "secret", "v1", 1000);
        expectWatchOn("/api/v1/services", Service.class, "service", "v1", 1000);

        operatorMain = new OperatorMain(() -> globalRegistry, kubeClient);
    }

    @Test
    void shouldRegisterPrometheusMeterRegistry() {
        // Given

        // When
        operatorMain.run();

        // Then
        verify(globalRegistry).add(argThat(PrometheusMeterRegistry.class::isInstance));
        mockServer.shutdown();
    }

    @Test
    void shouldRegisterOperatorMetrics() {
        // Given

        // When
        operatorMain.run();

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
        operatorMain.run();

        // Then
        assertThat(operatorMain.getRegistry().get("operator.sdk.reconciliations.executions.proxyreconciler").meter().getId()).isNotNull();
    }

    private void expectApiResources() {
        final APIResource kafkaProtocolFilterCrd = new APIResourceBuilder()
                .withCategories("kroxylicious-plugins")
                .withKind("KafkaProtocolFilter")
                .withName("kafkaprotocolfilters")
                .withNamespaced(true)
                .withShortNames("kpf")
                .withSingularName("kafkaprotocolfilter")
                .withVerbs("delete", "deletecollection", "get", "list", "patch", "create", "update", "watch")
                .build();

        final APIResourceList apiResourceList = new APIResourceListBuilder()
                .withGroupVersion("filter.kroxylicious.io/v1alpha1")
                .withApiVersion("v1")
                .addToResources(kafkaProtocolFilterCrd)
                .build();

        mockServer.expect()
                .get()
                .withPath("/apis/filter.kroxylicious.io/v1alpha1")
                .andReturn(HttpURLConnection.HTTP_OK, apiResourceList)
                .always();

        mockServer.expect()
                .get()
                .withPath("/apis/kroxylicious.io/v1alpha1/namespaces/randomNs/kafkaproxies")
                .andReturn(HttpURLConnection.HTTP_OK, new GenericKubernetesResourceBuilder()
                        .withKind("KafkProxy")
                        .withApiVersion("kroxylicious.io/v1alpha1")
                        .withNewMetadata()
                        .withName("random")
                        .withNamespace("randomNs")
                        .endMetadata()
                        .build())
                .always();
    }

    @SneakyThrows
    private <T extends HasMetadata> void expectWatchOn(String path, Class<T> resultType, String kind, String apiVersion, int delayMs) {
        mockServer.expect()
                .get()
                .withPath(path + "?resourceVersion=0")
                .andReturn(HttpURLConnection.HTTP_OK, getList(resultType, kind, apiVersion))
                .always();
        mockServer.expect()
                .withPath(path + "?allowWatchBookmarks=true&resourceVersion=" + START_RESOURCE_VERSION + "&timeoutSeconds=600&watch=true")
                .andUpgradeToWebSocket()
                .open()
                .waitFor(delayMs)
                .andEmit(new WatchEvent(new GenericKubernetesResourceBuilder()
                        .withKind(kind)
                        .withApiVersion(apiVersion)
                        .withNewMetadata()
                        .withName("random")
                        .withNamespace("randomNs")
                        .endMetadata()
                        .build(), "ADDED"))
                .done()
                .always();
    }

    @SuppressWarnings("unused")
    private <T extends HasMetadata> KubernetesList getList(Class<T> resourceClass, String kind, String apiVersion) {
        return new KubernetesListBuilder()
                .withKind(kind)
                .withApiVersion(apiVersion)
                .withMetadata(new ListMetaBuilder().withResourceVersion(START_RESOURCE_VERSION).build())
                .build();
    }
}
