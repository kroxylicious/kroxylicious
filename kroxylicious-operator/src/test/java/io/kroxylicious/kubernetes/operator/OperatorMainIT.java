/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import com.sun.net.httpserver.HttpServer;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class OperatorMainIT {
    // This is an IT because it depends on having a running Kube cluster

    private static final String CLUSTER_FOO_REF = "fooref";
    private static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    private static final String CLUSTER_BAR_REF = "barref";
    private static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";
    private static final String FOO = UUID.randomUUID().toString();

    private HttpServer managementServer;
    private KafkaProxy kafkaProxy;
    private OperatorMain operatorMain;

    @BeforeAll
    static void beforeAll() {
        LocallyRunOperatorExtension.applyCrd(KafkaProtocolFilter.class, OperatorTestUtils.kubeClient());
        LocallyRunOperatorExtension.applyCrd(KafkaProxy.class, OperatorTestUtils.kubeClient());
        LocallyRunOperatorExtension.applyCrd(VirtualKafkaCluster.class, OperatorTestUtils.kubeClient());
        LocallyRunOperatorExtension.applyCrd(KafkaService.class, OperatorTestUtils.kubeClient());
        LocallyRunOperatorExtension.applyCrd(KafkaProxyIngress.class, OperatorTestUtils.kubeClient());
    }

    @AfterAll
    static void afterAll() {
        try (KubernetesClient kubernetesClient = OperatorTestUtils.kubeClient()) {
            kubernetesClient.resources(KafkaProtocolFilter.class).delete();
            kubernetesClient.resources(KafkaProxyIngress.class).delete();
            kubernetesClient.resources(KafkaProxy.class).delete();
            kubernetesClient.resources(VirtualKafkaCluster.class).delete();
            kubernetesClient.resources(KafkaService.class).delete();
        }
    }

    @BeforeEach
    void beforeEach() throws Exception {
        managementServer = createManagementServer();
        operatorMain = new OperatorMain(managementServer, null, null);
    }

    @AfterEach
    void afterEach() {
        if (kafkaProxy != null) {
            final KubernetesClient kubernetesClient = Objects.requireNonNull(OperatorTestUtils.kubeClient());
            kubernetesClient.resource(kafkaProxy).delete();
            kubernetesClient.resource(clusterRef(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP)).delete();
            kubernetesClient.resource(clusterRef(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)).delete();
        }
        if (operatorMain != null) {
            operatorMain.stop();
        }
    }

    @Test
    void start() {
        assertThatNoException().isThrownBy(() -> operatorMain.start());
    }

    @Test
    void shouldRegisterPrometheusMetricsInGlobalRegistry() {
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
        kafkaProxy = createProxyInstance(proxyBuilder);

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

    @SuppressWarnings("resource")
    @Test
    void shouldMakeMetricsAvailableViaHttp() {
        // Given
        @SuppressWarnings("resource") // Only applies at JDK 21+ level and we are JDK 17
        HttpClient httpClient = HttpClient.newHttpClient();
        final HttpResponse.BodyHandler<Stream<String>> responseBodyHandler = HttpResponse.BodyHandlers.ofLines();
        final KafkaProxyBuilder proxyBuilder = new KafkaProxyBuilder()
                .withKind("KafkaProxy")
                .withNewMetadata()
                .withName("mycoolproxy")
                .endMetadata();
        operatorMain.start();

        // When
        kafkaProxy = createProxyInstance(proxyBuilder);

        // Then
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    final HttpResponse<Stream<String>> response = httpClient.send(
                            HttpRequest.newBuilder().uri(URI.create(managementAddress() + "/metrics")).build(),
                            responseBodyHandler);
                    assertThat(response.statusCode()).isEqualTo(200);
                    assertThat(response.body())
                            .isNotEmpty()
                            .anySatisfy(line -> assertThat(line).contains("kroxylicious_operator_build_info"))
                            .anySatisfy(line -> assertThat(line).contains("operator_sdk_reconciliations_executions_kafkaproxyreconciler"))
                            .anySatisfy(line -> assertThat(line).contains("operator_sdk_events_received"));
                });
    }

    @Test
    void shouldSendClientErrorForUnsupportedHttpMethod() throws IOException, InterruptedException {
        // Given
        @SuppressWarnings("resource") // Only applies at JDK 21+ level and we are JDK 17
        HttpClient httpClient = HttpClient.newHttpClient();
        final HttpResponse.BodyHandler<Stream<String>> responseBodyHandler = HttpResponse.BodyHandlers.ofLines();
        operatorMain.start();

        // When
        final HttpResponse<Stream<String>> response = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(managementAddress() + "/")).DELETE().build(),
                responseBodyHandler);

        // Then
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.body()).isEmpty();
    }

    @Test
    void shouldSendClientErrorForUnsupportedPaths() throws IOException, InterruptedException {
        // Given
        @SuppressWarnings("resource") // Only applies at JDK 21+ level and we are JDK 17
        HttpClient httpClient = HttpClient.newHttpClient();
        final HttpResponse.BodyHandler<Stream<String>> responseBodyHandler = HttpResponse.BodyHandlers.ofLines();
        operatorMain.start();

        // When
        final HttpResponse<Stream<String>> response = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(managementAddress() + "/")).GET().build(),
                responseBodyHandler);

        // Then
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).isEmpty();
    }

    @Test
    void shouldSendClientErrorForUnsupportedHttpMethodToMetrics() throws IOException, InterruptedException {
        // Given
        @SuppressWarnings("resource") // Only applies at JDK 21+ level and we are JDK 17
        HttpClient httpClient = HttpClient.newHttpClient();
        final HttpResponse.BodyHandler<Stream<String>> responseBodyHandler = HttpResponse.BodyHandlers.ofLines();
        operatorMain.start();

        // When
        final HttpResponse<Stream<String>> response = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(managementAddress() + "/metrics")).DELETE().build(),
                responseBodyHandler);

        // Then
        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.body()).isEmpty();
    }

    @Test
    void shouldWatchOnlyConfiguredNamespace() throws Exception {
        // Given
        operatorMain.stop();
        managementServer.stop(0);

        var kc = Objects.requireNonNull(OperatorTestUtils.kubeClient());
        var watchedNs = "watched-" + System.currentTimeMillis();
        var unwatchedNs = "unwatched-" + System.currentTimeMillis();
        var watched = kc.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(watchedNs).endMetadata().build()).create();
        var unwatched = kc.namespaces().resource(new NamespaceBuilder().withNewMetadata().withName(unwatchedNs).endMetadata().build()).create();
        var watchedKp = kc.resource(new KafkaProxyBuilder().withNewMetadata().withNamespace(watchedNs).withName("myproxy").endMetadata().withNewSpec().endSpec().build())
                .create();
        var unwatchedKp = kc
                .resource(new KafkaProxyBuilder().withNewMetadata().withNamespace(unwatchedNs).withName("myproxy").endMetadata().withNewSpec().endSpec().build())
                .create();

        managementServer = createManagementServer();
        operatorMain = new OperatorMain(managementServer, null, Set.of(watched.getMetadata().getName()));

        // When
        operatorMain.start();

        // Then
        Awaitility.await("reconcilesWatchedProxy")
                .untilAsserted(() -> {
                    var watchedProxy = kc.resource(watchedKp).get();
                    assertThat(watchedProxy)
                            .extracting(KafkaProxy::getStatus)
                            .extracting(KafkaProxyStatus::getObservedGeneration)
                            .withFailMessage("expecting watched proxy to have status with observed generation as operator should be reconciling it.")
                            .isNotNull();
                });

        var unwatchedProxy = kc.resource(unwatchedKp).get();
        assertThat(unwatchedProxy)
                .isNotNull()
                .extracting(KafkaProxy::getStatus)
                .withFailMessage("expecting unwatched proxy to have no status object as operator should not be reconciling it.")
                .isNull();

        Set.of(watched, unwatched).forEach(r -> kc.resource(r).delete());
    }

    private KafkaProxy createProxyInstance(KafkaProxyBuilder proxyBuilder) {
        final KubernetesClient kubernetesClient = Objects.requireNonNull(OperatorTestUtils.kubeClient());
        kubernetesClient.resource(clusterRef(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP)).create();
        kubernetesClient.resource(clusterRef(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)).create();
        return kubernetesClient.resource(proxyBuilder.build()).create();
    }

    private KafkaService clusterRef(String clusterRefName, String clusterBootstrap) {
        return new KafkaServiceBuilder().withNewMetadata().withName(clusterRefName).endMetadata()
                .withNewSpec()
                .withBootstrapServers(clusterBootstrap)
                .endSpec()
                .build();
    }

    private String managementAddress() {
        return String.format("http://%s:%s", managementServer.getAddress().getHostString(), managementServer.getAddress().getPort());
    }

    private static HttpServer createManagementServer() throws IOException {
        return HttpServer.create(new InetSocketAddress("localhost", 0), 10);
    }
}
