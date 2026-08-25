/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Updatable;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.Protocol;
import io.kroxylicious.kubernetes.api.common.ProxyRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaprotocolfilter.KafkaProtocolFilterReconciler;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconcilerIT;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxyingress.KafkaProxyIngressReconciler;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.KafkaServiceReconciler;
import io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster.VirtualKafkaClusterReconciler;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.testing.operator.ClusterUser;
import io.kroxylicious.testing.operator.ExternalOperator;
import io.kroxylicious.testing.operator.LocalKroxyliciousOperatorExtension;
import io.kroxylicious.testing.operator.OperatorTestUtils;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.STRIMZI_CLUSTER_CA_BUNDLE;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.testing.operator.OperatorTestUtils.uniqueSuffix;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * Integration test for the operator with all reconcilers wired together.
 * These tests focus on status conditions reported by the CR.
 * For deeper concerns, see the individual ReconcilerITs ({@link KafkaProxyReconcilerIT} etc.)
 *
 */
@EnabledIf(value = "io.kroxylicious.testing.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
@SuppressWarnings("java:S8692") // ITs run against a live API server; a fixed clock would be misleading since time is not controlled
class AllReconcilersIT {
    private static final String PROXY_A = "proxy-a";
    private static final String CLUSTER_FOO = "foo";
    private static final String CLUSTER_FOO_CLUSTER_IP_INGRESS = "foo-cluster-ip";
    private static final String CLUSTER_FOO_SERVICE = "foo-service";
    private static final String CLUSTER_FOO_FILTER = "foo-filter";
    private static final String STRIMZI_TLS_LISTENER = "tls";
    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    // KafkaServiceReconciler conditionally creates a Strimzi Kafka informer when it detects the
    // Strimzi API group. The CRD must be installed before the operator starts — setup/teardown
    // actions run at the right point in the extension lifecycle to guarantee this ordering.
    private static final SharedInformerManager sharedInformerManager = new SharedInformerManager(OperatorTestUtils.kubeClient(), Set.of());

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create(), sharedInformerManager))
            .withReconciler(new KafkaProxyIngressReconciler(Clock.systemUTC()))
            .withReconciler(new KafkaServiceReconciler(Clock.systemUTC(), sharedInformerManager))
            .withReconciler(new KafkaProtocolFilterReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR, sharedInformerManager))
            .withSetupAction(() -> {
                try (KubernetesClient client = OperatorTestUtils.kubeClient()) {
                    client.apiextensions().v1().customResourceDefinitions().resource(StrimziCrdUtils.kafkaCrd()).createOr(Updatable::update);
                }
            })
            .withTeardownAction(() -> {
                try (KubernetesClient client = OperatorTestUtils.kubeClient()) {
                    client.apiextensions().v1().customResourceDefinitions().resource(StrimziCrdUtils.kafkaCrd()).delete();
                }
            })
            .withAdditionalCleanupTypes(Kafka.class)
            .build();

    private ClusterUser clusterUser;
    private ExternalOperator externalOperator;

    @BeforeEach
    void setUp() {
        clusterUser = operator.clusterUser();
        externalOperator = operator.externalOperator();
    }

    @Test
    void emptyProxyIsAllowed() {
        // Given
        var suffix = uniqueSuffix();
        var myProxy = editableProxy(PROXY_A + suffix).build();

        // When
        createAll(myProxy);

        // Then
        assertResourceAttainsCondition(AllReconcilersIT::resourceReady, myProxy);
    }

    static Stream<Arguments> filterScenarios() {
        return Stream.of(
                argumentSet("no filters", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, KafkaProtocolFilter>) ((actor, suffix) -> null)),
                argumentSet("filter with simple config", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, KafkaProtocolFilter>) ((actor, suffix) -> {
                            var filter = editableFilter(CLUSTER_FOO_FILTER + suffix).build();
                            actor.create(filter);
                            return filter;
                        })),
                argumentSet("filter with config that refs a configmap", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, KafkaProtocolFilter>) ((actor, suffix) -> {
                        // @formatter:off
                            var filterConfigMap = new ConfigMapBuilder()
                                    .withNewMetadata()
                                    .withName("filter-configmap" + suffix)
                                    .endMetadata()
                                    .addToData("key", "value")
                                    .build();
                            var filter = editableFilter(CLUSTER_FOO_FILTER + suffix)
                                    .editOrNewSpec()
                                        .withConfigTemplate(Map.of("configMapProp", "${configmap:filter-configmap" + suffix + ":key}"))
                                    .endSpec()
                                    .build();
                            // @formatter:on
                            actor.create(filter);
                            actor.create(filterConfigMap);
                            return filter;
                        })));
    }

    @ParameterizedTest
    @MethodSource("filterScenarios")
    void singleVirtualCluster(String suffix, BiFunction<ClusterUser, String, KafkaProtocolFilter> filterFunc) {
        // Given
        var myProxy = editableProxy(PROXY_A + suffix).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TCP)
                    .endClusterIP()
                .endSpec()
                .build();
        // @formatter:on
        var myService = editableService(CLUSTER_FOO_SERVICE + suffix).build();

        var myFilter = filterFunc.apply(clusterUser, suffix);

        var myCluster = editableVirtualCluster(CLUSTER_FOO + suffix, myProxy, myService, List.of(myIngress), Optional.ofNullable(myFilter).stream().toList())
                .build();

        // When
        createAll(myProxy, myCluster, myIngress, myService);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myIngress, myService, myCluster);
        Optional.ofNullable(myFilter).map(f -> assertResourceAttainsCondition(AllReconcilersIT::refsResolved, f));

        assertResourceAttainsCondition(AllReconcilersIT::resourceReady, myProxy);

        assertResourceAttainsCondition(AllReconcilersIT::resourceAccepted, myCluster);
        // The accepted condition and ingresses may be set in separate reconciliation cycles,
        // so we wait explicitly for the ingresses to be populated rather than checking the
        // snapshot returned when the accepted condition first became true.
        AWAIT.alias("cluster %s has ingresses with bootstrap servers".formatted(CLUSTER_FOO + suffix))
                .untilAsserted(() -> assertThat(clusterUser.get(VirtualKafkaCluster.class, CLUSTER_FOO + suffix))
                        .isNotNull()
                        .extracting(VirtualKafkaCluster::getStatus)
                        .satisfies(vcs -> assertThat(vcs)
                                .extracting(VirtualKafkaClusterStatus::getIngresses, as(InstanceOfAssertFactories.list(Ingresses.class)))
                                .singleElement()
                                .extracting(Ingresses::getBootstrapServer, as(InstanceOfAssertFactories.STRING))
                                .isNotEmpty()));

    }

    @Test
    void downstreamOpenShiftRouteIngress() {
        assumeThat(OpenShiftUtils.supportsRoute())
                .withFailMessage("kubernetes server is missing support for resource kind Route").isTrue();

        // Given
        var suffix = uniqueSuffix();
        var domain = OpenShiftUtils.getDefaultIngressControllerDomain();
        var myProxy = editableProxy(PROXY_A + suffix).build();
        var myService = editableService(CLUSTER_FOO_SERVICE + suffix).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix, myProxy)
                .editOrNewSpec()
                    .withNewOpenShiftRoute()
                    .endOpenShiftRoute()
                    .withClusterIP(null)
                .endSpec()
                .build();
        var tlsCert = new SecretBuilder()
                .withNewMetadata()
                    .withName("downstream-tls-certificate" + suffix)
                .endMetadata()
                .withType("kubernetes.io/tls")
                .addToStringData("tls.crt", TestKeyMaterial.TEST_CERT_PEM)
                .addToStringData("tls.key", TestKeyMaterial.TEST_KEY_PEM)
                .build();
        var clusterIngress = new IngressesBuilder()
                .withIngressRef(new IngressRefBuilder().withName(name(myIngress)).build())
                .withNewTls()
                    .withNewCertificateRef()
                        .withName(name(tlsCert))
                    .endCertificateRef()
                .endTls()
                .build();
        var myCluster = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_FOO + suffix)
                .endMetadata()
                .withNewSpec()
                    .withNewProxyRef()
                        .withName(name(myProxy))
                    .endProxyRef()
                    .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName(name(myService)).build())
                    .withIngresses(List.of(clusterIngress))
                .endSpec()
                .build();
        // @formatter:on

        // When
        createAll(myProxy, myIngress, myService, tlsCert, myCluster);

        // Then
        assertResourceAttainsCondition(AllReconcilersIT::resourceAccepted, myCluster);
        AWAIT.alias("cluster %s has route-based bootstrap server".formatted(CLUSTER_FOO + suffix))
                .untilAsserted(() -> assertThat(clusterUser.get(VirtualKafkaCluster.class, CLUSTER_FOO + suffix))
                        .isNotNull()
                        .extracting(VirtualKafkaCluster::getStatus)
                        .satisfies(vcs -> assertThat(vcs)
                                .extracting(VirtualKafkaClusterStatus::getIngresses, as(InstanceOfAssertFactories.list(Ingresses.class)))
                                .singleElement()
                                .extracting(Ingresses::getBootstrapServer, as(InstanceOfAssertFactories.STRING))
                                .endsWith("." + domain + ":443")));
    }

    static Stream<Arguments> upstreamTlsScenarios() {
        return Stream.of(
                argumentSet("tls", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, Tls>) ((actor, suffix) -> new Tls())),
                argumentSet("tls with trust from secret", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, Tls>) ((actor, suffix) -> {
                        // @formatter:off
                            var trust = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("upstream-trust" + suffix)
                                    .endMetadata()
                                    .addToStringData("trust.pem", TestKeyMaterial.TEST_CERT_PEM)
                                    .build();
                            var ref = new io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder()
                                    .withNewTrustAnchorRef()
                                        .withNewRef()
                                          .withName(name(trust))
                                          .withKind("Secret")
                                        .endRef()
                                      .withKey("trust.pem")
                                    .endTrustAnchorRef()
                                    .build();
                            // @formatter:on
                            actor.create(trust);
                            return ref;
                        })),
                argumentSet("tls with trust from secret with store type", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, Tls>) ((actor, suffix) -> {
                        // @formatter:off
                            var trust = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("upstream-trust" + suffix)
                                    .endMetadata()
                                    .addToStringData("trust.crt", TestKeyMaterial.TEST_CERT_PEM)
                                    .build();
                            var ref = new io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder()
                                    .withNewTrustAnchorRef()
                                        .withNewRef()
                                          .withName(name(trust))
                                          .withKind("Secret")
                                        .endRef()
                                      .withStoreType("PEM")
                                      .withKey("trust.crt")
                                    .endTrustAnchorRef()
                                    .build();
                            // @formatter:on
                            actor.create(trust);
                            return ref;
                        })));
    }

    @ParameterizedTest
    @MethodSource("upstreamTlsScenarios")
    void upstreamTls(String suffix, BiFunction<ClusterUser, String, io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls> tlsFunc) {
        // Given
        var tlsScenario = tlsFunc.apply(clusterUser, suffix);

        var myProxy = editableProxy(PROXY_A + suffix).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TCP)
                    .endClusterIP()
                .endSpec()
                .build();

        var myService = editableService(CLUSTER_FOO_SERVICE + suffix)
                .editOrNewSpec()
                    .withTls(tlsScenario)
                .endSpec()
                .build();
        // @formatter:on

        var myCluster = editableVirtualCluster(CLUSTER_FOO + suffix, myProxy, myService, List.of(myIngress), List.of()).build();

        // When
        createAll(myProxy, myCluster, myIngress, myService);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::resourceReady, myProxy);
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myCluster, myIngress, myService);
    }

    static Stream<Arguments> downstreamTlsScenarios() {
        return Stream.of(
                argumentSet("tls with platform trust", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls>) ((actor, suffix) -> {
                        // @formatter:off
                            var downstreamCert = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-cert" + suffix)
                                    .endMetadata()
                                    .withType("kubernetes.io/tls")
                                    .addToStringData("tls.crt", TestKeyMaterial.TEST_CERT_PEM)
                                    .addToStringData("tls.key", TestKeyMaterial.TEST_KEY_PEM)
                                    .build();
                            // @formatter:on
                            actor.create(downstreamCert);
                            return new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder()
                                    .withNewCertificateRef()
                                    .withName(name(downstreamCert))
                                    .endCertificateRef()
                                    .build();
                        })),
                argumentSet("tls with trust from configmap", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls>) ((actor, suffix) -> {
                        // @formatter:off
                            var downstreamCert = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-cert" + suffix)
                                    .endMetadata()
                                    .withType("kubernetes.io/tls")
                                    .addToStringData("tls.crt", TestKeyMaterial.TEST_CERT_PEM)
                                    .addToStringData("tls.key", TestKeyMaterial.TEST_KEY_PEM)
                                    .build();
                            var downstreamTrust = new ConfigMapBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-trust-configmap" + suffix)
                                    .endMetadata()
                                    .addToData("trust.pem", TestKeyMaterial.TEST_CERT_PEM)
                                    .build();
                            var tls = new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder()
                                    .withNewCertificateRef()
                                        .withName(name(downstreamCert))
                                    .endCertificateRef()
                                    .editOrNewTrustAnchorRef()
                                        .withNewRef()
                                            .withName(name(downstreamTrust))
                                        .endRef()
                                        .withKey("trust.pem")
                                    .endTrustAnchorRef()
                                    .build();
                            // @formatter:on
                            actor.create(downstreamCert);
                            actor.create(downstreamTrust);
                            return tls;
                        })),
                argumentSet("tls with trust from secret", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls>) ((actor, suffix) -> {
                        // @formatter:off
                            var downstreamCert = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-cert" + suffix)
                                    .endMetadata()
                                    .withType("kubernetes.io/tls")
                                    .addToStringData("tls.crt", TestKeyMaterial.TEST_CERT_PEM)
                                    .addToStringData("tls.key", TestKeyMaterial.TEST_KEY_PEM)
                                    .build();
                            var downstreamTrust = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-trust-secret" + suffix)
                                    .endMetadata()
                                    .addToStringData("trust.pem", TestKeyMaterial.TEST_CERT_PEM)
                                    .build();
                            var tls = new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder()
                                    .withNewCertificateRef()
                                        .withName(name(downstreamCert))
                                    .endCertificateRef()
                                    .editOrNewTrustAnchorRef()
                                        .withNewRef()
                                            .withKind("Secret")
                                            .withName(name(downstreamTrust))
                                        .endRef()
                                        .withKey("trust.pem")
                                    .endTrustAnchorRef()
                                    .build();
                            // @formatter:on
                            actor.create(downstreamCert);
                            actor.create(downstreamTrust);
                            return tls;
                        })),
                argumentSet("tls with trust from configmap with new key of supported store type", uniqueSuffix(),
                        (BiFunction<ClusterUser, String, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls>) ((actor, suffix) -> {
                        // @formatter:off
                            var downstreamCert = new SecretBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-cert" + suffix)
                                    .endMetadata()
                                    .withType("kubernetes.io/tls")
                                    .addToStringData("tls.crt", TestKeyMaterial.TEST_CERT_PEM)
                                    .addToStringData("tls.key", TestKeyMaterial.TEST_KEY_PEM)
                                    .build();
                            var downstreamTrust = new ConfigMapBuilder()
                                    .withNewMetadata()
                                        .withName("downstream-trust-configmap" + suffix)
                                    .endMetadata()
                                    .addToData("trust.crt", TestKeyMaterial.TEST_CERT_PEM)
                                    .build();
                            var tls = new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder()
                                    .withNewCertificateRef()
                                        .withName(name(downstreamCert))
                                    .endCertificateRef()
                                    .editOrNewTrustAnchorRef()
                                        .withNewRef()
                                            .withName(name(downstreamTrust))
                                        .endRef()
                                        .withKey("trust.crt")
                                        .withStoreType("PEM")
                                    .endTrustAnchorRef()
                                    .build();
                            // @formatter:on
                            actor.create(downstreamCert);
                            actor.create(downstreamTrust);
                            return tls;
                        })));
    }

    @ParameterizedTest
    @MethodSource("downstreamTlsScenarios")
    void downstreamTls(String suffix, BiFunction<ClusterUser, String, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls> tlsFunc) {
        // Given
        var tlsScenario = tlsFunc.apply(clusterUser, suffix);

        var myProxy = editableProxy(PROXY_A + suffix).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TLS)
                    .endClusterIP()
                .endSpec()
                .build();

        var myService = editableService(CLUSTER_FOO_SERVICE + suffix).build();

        var myCluster = editableVirtualCluster(CLUSTER_FOO + suffix, myProxy, myService, List.of(myIngress), List.of())
                .editOrNewSpec()
                    .editIngress(0)
                        .withTls(tlsScenario)
                    .endIngress()
                .endSpec()
                .build();
        // @formatter:on

        // When
        createAll(myProxy, myCluster, myIngress, myService);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::resourceReady, myProxy);
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myCluster, myIngress, myService);
    }

    @Test
    void infrastructureAnnotationsAppliedToServices() {
        // Given
        var suffix = uniqueSuffix();
        var myProxy = editableProxy(PROXY_A + suffix).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix, myProxy)
                .editOrNewSpec()
                    .withNewInfrastructure()
                        .addToAnnotations("example.com/custom-annotation", "test-value")
                        .addToAnnotations("haproxy.router.openshift.io/timeout", "60s")
                    .endInfrastructure()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TCP)
                    .endClusterIP()
                .endSpec()
                .build();
        // @formatter:on

        var myService = editableService(CLUSTER_FOO_SERVICE + suffix).build();
        var myCluster = editableVirtualCluster(CLUSTER_FOO + suffix, myProxy, myService, List.of(myIngress), List.of()).build();

        // When
        createAll(myProxy, myIngress, myService, myCluster);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::resourceReady, myProxy);
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myCluster, myIngress, myService);
        assertResourceAttainsCondition(AllReconcilersIT::resourceAccepted, myCluster);

        // Verify Service has infrastructure annotations
        AWAIT.alias("Service for cluster %s has infrastructure annotations".formatted(CLUSTER_FOO + suffix))
                .untilAsserted(() -> {
                    String serviceName = CLUSTER_FOO + suffix + "-" + CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix + "-bootstrap";
                    var service = clusterUser.get(Service.class, serviceName);
                    assertThat(service)
                            .isNotNull()
                            .extracting(s -> s.getMetadata().getAnnotations())
                            .asInstanceOf(InstanceOfAssertFactories.MAP)
                            .containsEntry("example.com/custom-annotation", "test-value")
                            .containsEntry("haproxy.router.openshift.io/timeout", "60s")
                            .containsKey("kroxylicious.io/bootstrap-servers"); // operator annotation still present
                });
    }

    @Test
    void upstreamTlsFromStrimziKafkaRef() {
        // Given
        var suffix = uniqueSuffix();
        String kafkaName = "my-cluster" + suffix;
        // @formatter:off
        clusterUser.create(new KafkaBuilder()
                .withNewMetadata()
                    .withName(kafkaName)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .addNewListener()
                            .withName(STRIMZI_TLS_LISTENER)
                            .withPort(9093)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(true)
                        .endListener()
                    .endKafka()
                .endSpec()
                .build());

        // Patch the Kafka status to simulate what the Strimzi operator would do.
        // The Strimzi operator manages the status subresource of Kafka CRs, populating
        // listener addresses and other runtime state. In tests, we must manually set
        // this status since the Strimzi operator is not running.
        externalOperator.updateStatus(Kafka.class, kafkaName, fresh -> new KafkaBuilder(fresh)
                .withNewStatus()
                    .addNewListener()
                        .withName(STRIMZI_TLS_LISTENER)
                        .addNewAddress()
                                .withHost("kafka.example.com")
                                .withPort(9093)
                        .endAddress()
                    .endListener()
                .endStatus()
                .build());

        clusterUser.create(new SecretBuilder()
                .withNewMetadata()
                    .withName(kafkaName + STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX)
                .endMetadata()
                .addToData(STRIMZI_CLUSTER_CA_BUNDLE, "dGVzdC1jYQ==")
                .build());

        var myService = editableStrimziService(CLUSTER_FOO_SERVICE + suffix, kafkaName, STRIMZI_TLS_LISTENER).build();
        var myProxy = editableProxy(PROXY_A + suffix).build();
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS + suffix, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TCP)
                    .endClusterIP()
                .endSpec()
                .build();
        // @formatter:on

        var myCluster = editableVirtualCluster(CLUSTER_FOO + suffix, myProxy, myService, List.of(myIngress), List.of()).build();

        // When
        createAll(myProxy, myCluster, myIngress, myService);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::resourceReady, myProxy);
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myCluster, myIngress, myService);
    }

    private static KafkaServiceBuilder editableStrimziService(String name, String kafkaName, String listenerName) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withNewStrimziKafkaRef()
                        .withListenerName(listenerName)
                        .withTrustStrimziCaCertificate(true)
                        .withNewRef()
                            .withName(kafkaName)
                        .endRef()
                    .endStrimziKafkaRef()
                .endSpec();
        // @formatter:on
    }

    private void createAll(HasMetadata... resources) {
        Arrays.stream(resources).sequential().forEach(clusterUser::create);
    }

    private static KafkaProxyBuilder editableProxy(String name) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata();
        // @formatter:on
    }

    @SafeVarargs
    private <T extends CustomResource<?, ?>> void assertResourcesAttainCondition(Predicate<Condition> conditionPredicate, T... resources) {
        Arrays.stream(resources).forEach(resource -> assertResourceAttainsCondition(conditionPredicate, resource));
    }

    @SuppressWarnings("unchecked")
    private <T extends CustomResource<?, ?>> T assertResourceAttainsCondition(Predicate<Condition> conditionPredicate, T resource) {
        var result = new AtomicReference<T>();
        var name = name(resource);
        var clazz = resource.getClass();
        AWAIT.alias("resource %s (%s) meets predicate".formatted(name, clazz.getSimpleName()))
                .untilAsserted(() -> clusterUser.get(clazz, name),
                        actual -> {
                            assertThat(actual)
                                    .isNotNull()
                                    .extracting(CustomResource::getStatus)
                                    .isNotNull()
                                    .extracting("conditions", as(InstanceOfAssertFactories.list(Condition.class)))
                                    .filteredOn(conditionPredicate)
                                    .singleElement()
                                    .satisfies(readyCondition -> {
                                        assertThat(readyCondition.getStatus()).isEqualTo(Condition.Status.TRUE);
                                    });
                            result.set((T) actual);
                        });
        return result.get();
    }

    private static VirtualKafkaClusterBuilder editableVirtualCluster(String clusterName, KafkaProxy proxy, KafkaService service, List<KafkaProxyIngress> ingresses,
                                                                     List<KafkaProtocolFilter> filters) {
        var ingressRefs = ingresses.stream().map(i -> new IngressesBuilder().withNewIngressRef().withName(name(i)).endIngressRef().build()).toList();
        var filterRefs = filters.stream().map(f -> new FilterRefBuilder().withName(name(f)).build()).toList();

        var build = new KafkaServiceRefBuilder().withName(name(service)).build();
        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                .endMetadata()
                .withNewSpec()
                    .withNewProxyRef()
                        .withName(name(proxy))
                    .endProxyRef()
                    .withTargetKafkaServiceRef(build)
                    .withIngresses(ingressRefs)
                    .withFilterRefs(filterRefs)
                .endSpec();
        // @formatter:on
    }

    private static KafkaProxyIngressBuilder editableIngress(String name, KafkaProxy proxy) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withProxyRef(new ProxyRefBuilder().withName(name(proxy)).build())
                .endSpec();
        // @formatter:on

    }

    private static KafkaServiceBuilder editableService(String name) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("example.com:5555")
                    .withNodeIdRanges(new NodeIdRangesBuilder().withStart(0L).withEnd(0L).build())
                .endSpec();
        // @formatter:on
    }

    private static KafkaProtocolFilterBuilder editableFilter(String name) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withType("Type")
                    .withConfigTemplate(Map.of())
                .endSpec();
        // @formatter:on
    }

    private static boolean resourceReady(Condition c) {
        return Condition.Type.Ready.equals(c.getType()) && Condition.Status.TRUE.equals(c.getStatus());
    }

    private static boolean resourceAccepted(Condition c) {
        return Condition.Type.Accepted.equals(c.getType()) && Condition.Status.TRUE.equals(c.getStatus());
    }

    private static boolean refsResolved(Condition c) {
        return Condition.Type.ResolvedRefs.equals(c.getType()) && Condition.Status.TRUE.equals(c.getStatus());
    }
}
