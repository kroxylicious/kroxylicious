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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
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
import io.kroxylicious.kubernetes.operator.LocallyRunningOperatorRbacHandler.TestActor;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * Integration test for the operator with all reconcilers wired together.
 * These tests focus on status conditions reported by the CR.
 * For deeper concerns, see the individual ReconcilerITs ({@link KafkaProxyReconcilerIT} etc.)
 *
 */
@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class AllReconcilersIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AllReconcilersIT.class);
    private static final String PROXY_A = "proxy-a";
    private static final String CLUSTER_FOO = "foo";
    private static final String CLUSTER_FOO_CLUSTER_IP_INGRESS = "foo-cluster-ip";
    private static final String CLUSTER_FOO_SERVICE = "foo-service";
    private static final String CLUSTER_FOO_FILTER = "foo-filter";
    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler(TestFiles.INSTALL_MANIFESTS_DIR, "*.ClusterRole.*.yaml");

    @RegisterExtension
    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect, so we can use it as an instance field.
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create()))
            .withReconciler(new KafkaProxyIngressReconciler(Clock.systemUTC()))
            .withReconciler(new KafkaServiceReconciler(Clock.systemUTC()))
            .withReconciler(new KafkaProtocolFilterReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withKubernetesClient(rbacHandler.operatorClient())
            .waitForNamespaceDeletion(false)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();
    private final TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void emptyProxyIsAllowed() {
        // Given

        var myProxy = editableProxy(PROXY_A).build();

        // When
        createAll(myProxy);

        // Then
        assertResourceAttainsCondition(AllReconcilersIT::resourceReady, myProxy);
    }

    static Stream<Arguments> filterScenarios() {
        return Stream.of(
                argumentSet("no filters", (Function<TestActor, KafkaProtocolFilter>) (builder -> null)),
                argumentSet("filter with simple config", (Function<TestActor, KafkaProtocolFilter>) (actor -> {
                    var filter = editableFilter(CLUSTER_FOO_FILTER).build();
                    actor.create(filter);
                    return filter;
                })),
                argumentSet("filter with config that refs a configmap", (Function<TestActor, KafkaProtocolFilter>) (actor -> {
                // @formatter:off
                    var filterConfigMap = new ConfigMapBuilder()
                            .withNewMetadata()
                            .withName("filter-configmap")
                            .endMetadata()
                            .addToData("key", "value")
                            .build();
                    var filter = editableFilter(CLUSTER_FOO_FILTER)
                            .editOrNewSpec()
                                .withConfigTemplate(Map.of("configMapProp", "${configmap:filter-configmap:key}"))
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
    void singleVirtualCluster(Function<TestActor, KafkaProtocolFilter> filterFunc) {
        // Given
        var myProxy = editableProxy(PROXY_A).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TCP)
                    .endClusterIP()
                .endSpec()
                .build();
        // @formatter:on
        var myService = editableService(CLUSTER_FOO_SERVICE).build();

        var myFilter = filterFunc.apply(testActor);

        var myCluster = editableVirtualCluster(CLUSTER_FOO, myProxy, myService, List.of(myIngress), Optional.ofNullable(myFilter).stream().toList()).build();

        // When
        createAll(myProxy, myCluster, myIngress, myService);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myIngress, myService, myCluster);
        Optional.ofNullable(myFilter).map(f -> assertResourceAttainsCondition(AllReconcilersIT::refsResolved, f));

        assertResourceAttainsCondition(AllReconcilersIT::resourceReady, myProxy);

        var acceptedCluster = assertResourceAttainsCondition(AllReconcilersIT::resourceAccepted, myCluster);
        assertThat(acceptedCluster)
                .extracting(VirtualKafkaCluster::getStatus)
                .satisfies(vcs -> {
                    assertThat(vcs)
                            .extracting(VirtualKafkaClusterStatus::getIngresses, as(InstanceOfAssertFactories.list(Ingresses.class)))
                            .singleElement()
                            .extracting(Ingresses::getBootstrapServer, as(InstanceOfAssertFactories.STRING))
                            .isNotEmpty();
                });

    }

    static Stream<Arguments> upstreamTlsScenarios() {
        return Stream.of(
                argumentSet("tls", (Function<TestActor, Tls>) (builder -> new Tls())),
                argumentSet("tls with trust from secret", (Function<TestActor, Tls>) (actor -> {
                // @formatter:off
                    var trust = new SecretBuilder()
                            .withNewMetadata()
                                .withName("upstream-trust")
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
                })));
    }

    @ParameterizedTest
    @MethodSource("upstreamTlsScenarios")
    void upstreamTls(Function<TestActor, io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls> tlsFunc) {
        // Given
        var tlsScenario = tlsFunc.apply(testActor);

        var myProxy = editableProxy(PROXY_A).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TCP)
                    .endClusterIP()
                .endSpec()
                .build();

        var myService = editableService(CLUSTER_FOO_SERVICE)
                .editOrNewSpec()
                    .withTls(tlsScenario)
                .endSpec()
                .build();
        // @formatter:on

        var myCluster = editableVirtualCluster(CLUSTER_FOO, myProxy, myService, List.of(myIngress), List.of()).build();

        // When
        createAll(myProxy, myCluster, myIngress, myService);

        // Then
        assertResourcesAttainCondition(AllReconcilersIT::resourceReady, myProxy);
        assertResourcesAttainCondition(AllReconcilersIT::refsResolved, myCluster, myIngress, myService);
    }

    static Stream<Arguments> downstreamTlsScenarios() {
        // @formatter:off
        var downstreamCert = new SecretBuilder()
                .withNewMetadata()
                    .withName("downstream-cert")
                .endMetadata()
                .withType("kubernetes.io/tls")
                .addToStringData("tls.crt", TestKeyMaterial.TEST_CERT_PEM)
                .addToStringData("tls.key", TestKeyMaterial.TEST_KEY_PEM)
                .build();
        var downstreamTls = new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder()
                .withNewCertificateRef()
                    .withName(name(downstreamCert))
                .endCertificateRef()
                .build();
        // @formatter:on

        return Stream.of(
                argumentSet("tls with platform trust", (Function<TestActor, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls>) (actor -> {
                    actor.create(downstreamCert);
                    return new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder(downstreamTls).build();
                })),
                argumentSet("tls with trust from configmap",
                        (Function<TestActor, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls>) (actor -> {

                        // @formatter:off
                    var downstreamTrust = new ConfigMapBuilder()
                            .withNewMetadata()
                                .withName("downstream-trust")
                            .endMetadata()
                            .addToData("trust.pem", TestKeyMaterial.TEST_CERT_PEM)
                            .build();
                    var tls = new io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder(downstreamTls)
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
                        })));
    }

    @ParameterizedTest
    @MethodSource("downstreamTlsScenarios")
    void downstreamTls(Function<TestActor, io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls> tlsFunc) {
        // Given
        var tlsScenario = tlsFunc.apply(testActor);

        var myProxy = editableProxy(PROXY_A).build();
        // @formatter:off
        var myIngress = editableIngress(CLUSTER_FOO_CLUSTER_IP_INGRESS, myProxy)
                .editOrNewSpec()
                    .withNewClusterIP()
                        .withProtocol(Protocol.TLS)
                    .endClusterIP()
                .endSpec()
                .build();

        var myService = editableService(CLUSTER_FOO_SERVICE).build();

        var myCluster = editableVirtualCluster(CLUSTER_FOO, myProxy, myService, List.of(myIngress), List.of())
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

    private void createAll(HasMetadata... resources) {
        Arrays.stream(resources).sequential().forEach(testActor::create);
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
                .untilAsserted(() -> testActor.get(clazz, name),
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
