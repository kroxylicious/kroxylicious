/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.Protocol;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.ProxyConfigDependentResource;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.testing.operator.ClusterUser;
import io.kroxylicious.testing.operator.ExternalOperator;
import io.kroxylicious.testing.operator.LocalKroxyliciousOperatorExtension;
import io.kroxylicious.testing.operator.OperatorTestUtils;
import io.kroxylicious.testing.operator.assertj.ConditionListAssert;
import io.kroxylicious.testing.operator.assertj.VirtualKafkaClusterStatusAssert;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.common.Protocol.TCP;
import static io.kroxylicious.kubernetes.api.common.Protocol.TLS;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.generation;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.testing.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
@SuppressWarnings("java:S8692") // ITs run against a live API server; a fixed clock would be misleading since time is not controlled
class VirtualKafkaClusterReconcilerIT {

    private static final String BOOTSTRAP_SERVERS = "foo.bootstrap:9090";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    private static final SharedInformerManager sharedInformerManager = new SharedInformerManager(OperatorTestUtils.kubeClient(), Set.of());

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create(), sharedInformerManager))
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .replaceClusterRoleGlobs("*.ClusterRole*.yaml")
            .build();

    private ClusterUser clusterUser;
    private ExternalOperator externalOperator;

    // unique per-test resource names to avoid stale reconciler events from a previous test
    // racing against resources created by the next test with the same name (issue #4746)
    private String proxyA;
    private String proxyB;
    private String barCluster;
    private String ingressD;
    private String ingressE;
    private String serviceH;
    private String filterK;
    private String secretName;
    private String trustAnchorSecretName;

    @BeforeEach
    void setUp() {
        clusterUser = operator.clusterUser();
        externalOperator = operator.externalOperator();
        var suffix = UUID.randomUUID().toString().substring(0, 8);
        proxyA = "proxy-a-" + suffix;
        proxyB = "proxy-b-" + suffix;
        barCluster = "bar-cluster-" + suffix;
        ingressD = "ingress-d-" + suffix;
        ingressE = "ingress-e-" + suffix;
        serviceH = "service-h-" + suffix;
        filterK = "service-k-" + suffix;
        secretName = "cert-" + suffix;
        trustAnchorSecretName = "my-secret-" + suffix;
    }

    @Test
    void shouldResolveWhenClusterCreatedAfterReferents() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(filter(filterK)));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(barCluster, proxyA, ingressD, serviceH, filterK));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileProxyInitiallyAbsent() {
        // Given
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(barCluster, proxyA, ingressD, serviceH, null));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        clusterUser.create(kafkaProxy(proxyA));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileServiceInitiallyAbsent() {
        // Given
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        clusterUser.create(kafkaProxy(proxyA));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(barCluster, proxyA, ingressD, serviceH, null));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileIngressInitiallyAbsent() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);

        // When
        VirtualKafkaCluster resource = cluster(barCluster, proxyA, ingressD, serviceH, null);
        VirtualKafkaCluster clusterBar = clusterUser.create(resource);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileFilterInitiallyAbsent() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(barCluster, proxyA, ingressD, serviceH, filterK));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(clusterUser.create(filter(filterK)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhenProxyDeleted() {
        // Given
        KafkaProxy proxy = clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(filter(filterK)));
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(barCluster, proxyA, ingressD, serviceH, filterK));
        assertAllConditionsTrue(clusterBar);

        // When
        clusterUser.delete((HasMetadata) proxy);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);
    }

    @Test
    void shouldNotResolveWhenFilterDeleted() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        var filter = updateStatusObservedGeneration(clusterUser.create(filter(filterK)));
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(barCluster, proxyA, ingressD, serviceH, filterK));
        assertAllConditionsTrue(clusterBar);

        // When
        clusterUser.delete(filter);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);
    }

    @Test
    void shouldNotResolveWhileIngressRefersToOtherProxy() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        clusterUser.create(kafkaProxy(proxyB));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyB, TCP))); // not A, which is what the VKC references

        // When
        VirtualKafkaCluster resource = cluster(barCluster, proxyA, ingressD, serviceH, null);
        VirtualKafkaCluster clusterBar = clusterUser.create(resource);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_TRANSITIVE_REFS_NOT_FOUND);

        // And when
        updateStatusObservedGeneration(clusterUser.replace(clusterIpIngress(ingressD, proxyA, TCP)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileTwoIpIngresses() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressD, proxyA, TCP)));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(ingressE, proxyA, TCP)));

        // When
        VirtualKafkaCluster resource = cluster(barCluster, proxyA, List.of(ingressD, ingressE), serviceH, null);
        VirtualKafkaCluster clusterBar = clusterUser.create(resource);

        // Then
        assertClusterAcceptedFalse(clusterBar, ProxyConfigDependentResource.REASON_INVALID);

        // And when
        clusterUser.replace(cluster(barCluster, proxyA, List.of(ingressD), serviceH, null));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldReportIngressClusterIpBootstrap() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        var cluster = cluster(barCluster, proxyA, ingressD, serviceH, null);
        var ingress = clusterIpIngress(ingressD, proxyA, TCP);
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, barCluster + "-" + ingressD + "-bootstrap.%s.svc.cluster.local:9292", Protocol.TCP);
    }

    @Test
    void shouldResolveWithSecretTrustAnchorRef() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        // @formatter:off
        var ingresses = List.of(new IngressesBuilder()
                .withNewIngressRef()
                    .withName(ingressD)
                .endIngressRef()
                .withNewTls()
                    .withNewCertificateRef()
                        .withName(secretName)
                    .endCertificateRef()
                .withNewTrustAnchorRef()
                    .withNewRef()
                        .withKind("Secret")
                        .withName(trustAnchorSecretName)
                    .endRef()
                    .withKey("cert.pem")
                .endTrustAnchorRef()
                .endTls()
                .build());

        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(barCluster)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(proxyA)
                .endProxyRef()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef()
                    .withName(serviceH)
                .endTargetKafkaServiceRef();
        // @formatter:on
        var cluster = specBuilder.endSpec().build();
        var ingress = clusterIpIngress(ingressD, proxyA, TLS);
        clusterUser.create(tlsKeyAndCertSecret(secretName));
        clusterUser.create(secretTrustAnchorRef(trustAnchorSecretName));

        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertAllConditionsTrue(clusterBar);
        assertClusterIngressStatusPopulated(clusterBar, ingress, barCluster + "-" + ingressD + "-bootstrap.%s.svc.cluster.local:9292", Protocol.TLS);
    }

    @Test
    void shouldReportIngressTlsClusterIpBootstrap() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        // @formatter:off
        var ingresses = List.of(new IngressesBuilder()
                        .withNewIngressRef()
                            .withName(ingressD)
                        .endIngressRef()
                        .withNewTls()
                            .withNewCertificateRef()
                                .withName(secretName)
                            .endCertificateRef()
                        .endTls()
                        .build());
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(barCluster)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(proxyA)
                .endProxyRef()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef()
                    .withName(serviceH)
                .endTargetKafkaServiceRef();
        // @formatter:on
        var cluster = specBuilder.endSpec().build();
        var ingress = clusterIpIngress(ingressD, proxyA, TLS);
        clusterUser.create(tlsKeyAndCertSecret(secretName));
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, barCluster + "-" + ingressD + "-bootstrap.%s.svc.cluster.local:9292", Protocol.TLS);
    }

    @Test
    void shouldReportIngressLoadBalancerBootstrap() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        clusterUser.create(tlsKeyAndCertSecret(secretName));
        // @formatter:off
        var ingresses = new IngressesBuilder()
                .withNewIngressRef()
                    .withName(ingressD)
                .endIngressRef()
                .withNewTls()
                    .withNewCertificateRef()
                        .withName(secretName)
                    .endCertificateRef()
                .endTls()
                .build();
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(barCluster)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(proxyA)
                .endProxyRef()
                .withIngresses(List.of(ingresses))
                .withNewTargetKafkaServiceRef()
                    .withName(serviceH)
                .endTargetKafkaServiceRef();
        // @formatter:on
        var cluster = specBuilder.endSpec().build();
        var ingress = loadBalancerIngress(ingressD, proxyA);
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bootstrap.kafka:9083", Protocol.TLS);
    }

    @Test
    void shouldReportIngressClusterIpBootstrapWhenIngressInitiallyAbsent() {
        // Given
        clusterUser.create(kafkaProxy(proxyA));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(serviceH)), BOOTSTRAP_SERVERS);
        var cluster = cluster(barCluster, proxyA, ingressD, serviceH, null);
        var ingress = clusterIpIngress(ingressD, proxyA, TCP);

        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        AWAIT.alias("ClusterStatusBootstrapNotPresent").untilAsserted(() -> {
            var vkc = clusterUser.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(clusterBar)).get();
            VirtualKafkaClusterStatus status = vkc.getStatus();
            assertThat(status)
                    .isNotNull()
                    .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                    .isEmpty();
        });

        // When
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, barCluster + "-" + ingressD + "-bootstrap.%s.svc.cluster.local:9292", Protocol.TCP);
    }

    private VirtualKafkaCluster cluster(String clusterName, String proxyName, String ingressName, String serviceName, @Nullable String filterName) {
        return cluster(clusterName, proxyName, List.of(ingressName), serviceName, filterName);
    }

    private VirtualKafkaCluster cluster(String clusterName, String proxyName, List<String> ingressNamees, String serviceName, @Nullable String filterName) {
        var ingresses = ingressNamees.stream().map(name -> new IngressesBuilder().withNewIngressRef().withName(name).endIngressRef().build()).toList();
        // @formatter:off
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(proxyName)
                .endProxyRef()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef()
                    .withName(serviceName)
                .endTargetKafkaServiceRef();
        if (filterName != null) {
            // filters are optional
            specBuilder.addNewFilterRef()
                    .withName(filterName)
                .endFilterRef();
        }
        // @formatter:on
        return specBuilder.endSpec().build();
    }

    private void assertClusterResolvedRefsFalse(VirtualKafkaCluster cr, String expectedReason) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = clusterUser.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList()
                    .singleOfType(Condition.Type.ResolvedRefs)
                    .hasStatus(Condition.Status.FALSE)
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .hasReason(expectedReason);
        });
    }

    private void assertAllConditionsTrue(VirtualKafkaCluster cr) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = clusterUser.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            ConditionListAssert conditionListAssert = VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList();
            conditionListAssert
                    .singleOfType(Condition.Type.ResolvedRefs)
                    .isResolvedRefsTrue(vkc);
            conditionListAssert
                    .singleOfType(Condition.Type.Accepted)
                    .isAcceptedTrue(vkc);
        });
    }

    private void assertClusterAcceptedFalse(VirtualKafkaCluster cr,
                                            String expectedReason) {
        AWAIT.alias("ClusterStatusResolvedRefs").untilAsserted(() -> {
            var vkc = clusterUser.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(vkc.getStatus()).isNotNull();
            VirtualKafkaClusterStatusAssert
                    .assertThat(vkc.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .conditionList()
                    .singleOfType(Condition.Type.Accepted)
                    .hasStatus(Condition.Status.FALSE)
                    .hasObservedGenerationInSyncWithMetadataOf(vkc)
                    .hasReason(expectedReason);
        });
    }

    private void assertClusterIngressStatusPopulated(VirtualKafkaCluster clusterBar, KafkaProxyIngress ingress, String expectedBootstrapServer, Protocol protocol) {
        AWAIT.alias("ClusterIngressStatus").untilAsserted(() -> {
            var vkc = clusterUser.resources(VirtualKafkaCluster.class)
                    .withName(ResourcesUtil.name(clusterBar)).get();
            var status = vkc.getStatus();
            assertThat(status)
                    .isNotNull()
                    .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                    .singleElement()
                    .satisfies(i -> {
                        assertThat(i.getName()).isEqualTo(ResourcesUtil.name(ingress));
                        assertThat(i.getBootstrapServer()).isEqualTo(expectedBootstrapServer.formatted(operator.getNamespace()));
                        assertThat(i.getProtocol()).isEqualTo(protocol);
                    });
        });
    }

    KafkaProxy kafkaProxy(String name) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .build();
        // @formatter:on
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, String proxyName, Protocol protocol) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(ingressName)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterIP()
                        .withProtocol(protocol)
                    .endClusterIP()
                    .withNewProxyRef()
                        .withName(proxyName)
                    .endProxyRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    private Secret tlsKeyAndCertSecret(String name) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withType("kubernetes.io/tls")
                .addToData("tls.crt", "whatever")
                .addToData("tls.key", "whatever")
                .build();
    }

    private Secret secretTrustAnchorRef(String name) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .addToData("cert.pem", "whatever")
                .build();
    }

    private KafkaProxyIngress loadBalancerIngress(String ingressName, String proxyName) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(ingressName)
                .endMetadata()
                .withNewSpec()
                    .withNewLoadBalancer()
                        .withBootstrapAddress("bootstrap.kafka")
                        .withAdvertisedBrokerAddressPattern("broker-$(nodeId).kafka")
                    .endLoadBalancer()
                    .withNewProxyRef()
                        .withName(proxyName)
                    .endProxyRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaService(String name) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .editOrNewSpec()
                .withBootstrapServers("foo.bootstrap:9090")
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaProtocolFilter filter(String name) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .editOrNewSpec()
                .withType("com.example.Filter")
                .withConfigTemplate(Map.of())
                .endSpec()
                .build();
        // @formatter:on
    }

    private KafkaProtocolFilter updateStatusObservedGeneration(KafkaProtocolFilter filter) {
        return externalOperator.updateStatus(KafkaProtocolFilter.class, name(filter), fresh -> {
            fresh.setStatus(new KafkaProtocolFilterStatusBuilder().withObservedGeneration(generation(fresh)).build());
            return fresh;
        });
    }

    private KafkaService updateStatusObservedGeneration(KafkaService service, String bootstrapServers) {
        return externalOperator.updateStatus(KafkaService.class, name(service), fresh -> {
            fresh.setStatus(new KafkaServiceStatusBuilder().withObservedGeneration(generation(fresh))
                    .withBootstrapServers(bootstrapServers)
                    .build());
            return fresh;
        });
    }

    private KafkaProxyIngress updateStatusObservedGeneration(KafkaProxyIngress ingress) {
        return externalOperator.updateStatus(KafkaProxyIngress.class, name(ingress), fresh -> {
            fresh.setStatus(new KafkaProxyIngressStatusBuilder().withObservedGeneration(generation(fresh)).build());
            return fresh;
        });
    }

}
