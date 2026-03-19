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
import io.kroxylicious.kubernetes.operator.ClusterUser;
import io.kroxylicious.kubernetes.operator.LocalKroxyliciousOperatorExtension;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.ProxyConfigDependentResource;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.common.Protocol.TCP;
import static io.kroxylicious.kubernetes.api.common.Protocol.TLS;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.generation;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class VirtualKafkaClusterReconcilerIT {

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_BAR = "bar-cluster";
    private static final String INGRESS_D = "ingress-d";
    private static final String INGRESS_E = "ingress-e";
    private static final String SERVICE_H = "service-h";
    private static final String FILTER_K = "service-k";
    private static final String BOOTSTRAP_SERVERS = "foo.bootstrap:9090";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));
    public static final String SECRET_NAME = "cert";
    public static final String SECRET_TRUST_ANCHOR_REF_NAME = "my-secret";

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create()))
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .replaceClusterRoleGlobs("*.ClusterRole*.yaml")
            .build();

    private ClusterUser clusterUser;

    @BeforeEach
    void setUp() {
        clusterUser = operator.clusterUser();
    }

    @Test
    void shouldResolveWhenClusterCreatedAfterReferents() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(filter(FILTER_K)));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileProxyInitiallyAbsent() {
        // Given
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        clusterUser.create(kafkaProxy(PROXY_A));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileServiceInitiallyAbsent() {
        // Given
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        clusterUser.create(kafkaProxy(PROXY_A));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileIngressInitiallyAbsent() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        VirtualKafkaCluster clusterBar = clusterUser.create(resource);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileFilterInitiallyAbsent() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);

        // And When
        updateStatusObservedGeneration(clusterUser.create(filter(FILTER_K)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhenProxyDeleted() {
        // Given
        KafkaProxy proxy = clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(filter(FILTER_K)));
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));
        assertAllConditionsTrue(clusterBar);

        // When
        clusterUser.delete((HasMetadata) proxy);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);
    }

    @Test
    void shouldNotResolveWhenFilterDeleted() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        var filter = updateStatusObservedGeneration(clusterUser.create(filter(FILTER_K)));
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, FILTER_K));
        assertAllConditionsTrue(clusterBar);

        // When
        clusterUser.delete(filter);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_REFS_NOT_FOUND);
    }

    @Test
    void shouldNotResolveWhileIngressRefersToOtherProxy() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        clusterUser.create(kafkaProxy(PROXY_B));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_B, TCP))); // not A, which is what the VKC references

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        VirtualKafkaCluster clusterBar = clusterUser.create(resource);

        // Then
        assertClusterResolvedRefsFalse(clusterBar, Condition.REASON_TRANSITIVE_REFS_NOT_FOUND);

        // And when
        updateStatusObservedGeneration(clusterUser.replace(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldNotResolveWhileTwoIpIngresses() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_D, PROXY_A, TCP)));
        updateStatusObservedGeneration(clusterUser.create(clusterIpIngress(INGRESS_E, PROXY_A, TCP)));

        // When
        VirtualKafkaCluster resource = cluster(CLUSTER_BAR, PROXY_A, List.of(INGRESS_D, INGRESS_E), SERVICE_H, null);
        VirtualKafkaCluster clusterBar = clusterUser.create(resource);

        // Then
        assertClusterAcceptedFalse(clusterBar, ProxyConfigDependentResource.REASON_INVALID);

        // And when
        clusterUser.replace(cluster(CLUSTER_BAR, PROXY_A, List.of(INGRESS_D), SERVICE_H, null));

        // Then
        assertAllConditionsTrue(clusterBar);
    }

    @Test
    void shouldReportIngressClusterIpBootstrap() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        var cluster = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        var ingress = clusterIpIngress(INGRESS_D, PROXY_A, TCP);
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bar-cluster-ingress-d-bootstrap.%s.svc.cluster.local:9292", Protocol.TCP);
    }

    @Test
    void shouldResolveWithSecretTrustAnchorRef() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        // @formatter:off
        var ingresses = List.of(new IngressesBuilder()
                .withNewIngressRef()
                    .withName(INGRESS_D)
                .endIngressRef()
                .withNewTls()
                    .withNewCertificateRef()
                        .withName(SECRET_NAME)
                    .endCertificateRef()
                .withNewTrustAnchorRef()
                    .withNewRef()
                        .withKind("Secret")
                        .withName(SECRET_TRUST_ANCHOR_REF_NAME)
                    .endRef()
                    .withKey("cert.pem")
                .endTrustAnchorRef()
                .endTls()
                .build());

        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_BAR)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(PROXY_A)
                .endProxyRef()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef()
                    .withName(SERVICE_H)
                .endTargetKafkaServiceRef();
        // @formatter:on
        var cluster = specBuilder.endSpec().build();
        var ingress = clusterIpIngress(INGRESS_D, PROXY_A, TLS);
        clusterUser.create(tlsKeyAndCertSecret(SECRET_NAME));
        clusterUser.create(secretTrustAnchorRef(SECRET_TRUST_ANCHOR_REF_NAME));

        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertAllConditionsTrue(clusterBar);
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bar-cluster-ingress-d-bootstrap.%s.svc.cluster.local:9292", Protocol.TLS);
    }

    @Test
    void shouldReportIngressTlsClusterIpBootstrap() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        // @formatter:off
        var ingresses = List.of(new IngressesBuilder()
                        .withNewIngressRef()
                            .withName(INGRESS_D)
                        .endIngressRef()
                        .withNewTls()
                            .withNewCertificateRef()
                                .withName(SECRET_NAME)
                            .endCertificateRef()
                        .endTls()
                        .build());
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_BAR)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(PROXY_A)
                .endProxyRef()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef()
                    .withName(SERVICE_H)
                .endTargetKafkaServiceRef();
        // @formatter:on
        var cluster = specBuilder.endSpec().build();
        var ingress = clusterIpIngress(INGRESS_D, PROXY_A, TLS);
        clusterUser.create(tlsKeyAndCertSecret(SECRET_NAME));
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bar-cluster-ingress-d-bootstrap.%s.svc.cluster.local:9292", Protocol.TLS);
    }

    @Test
    void shouldReportIngressLoadBalancerBootstrap() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        clusterUser.create(tlsKeyAndCertSecret(SECRET_NAME));
        // @formatter:off
        var ingresses = new IngressesBuilder()
                .withNewIngressRef()
                    .withName(INGRESS_D)
                .endIngressRef()
                .withNewTls()
                    .withNewCertificateRef()
                        .withName(SECRET_NAME)
                    .endCertificateRef()
                .endTls()
                .build();
        var specBuilder = new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(CLUSTER_BAR)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                    .withName(PROXY_A)
                .endProxyRef()
                .withIngresses(List.of(ingresses))
                .withNewTargetKafkaServiceRef()
                    .withName(SERVICE_H)
                .endTargetKafkaServiceRef();
        // @formatter:on
        var cluster = specBuilder.endSpec().build();
        var ingress = loadBalancerIngress(INGRESS_D, PROXY_A);
        updateStatusObservedGeneration(clusterUser.create(ingress));

        // When
        VirtualKafkaCluster clusterBar = clusterUser.create(cluster);

        // Then
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bootstrap.kafka:9083", Protocol.TLS);
    }

    @Test
    void shouldReportIngressClusterIpBootstrapWhenIngressInitiallyAbsent() {
        // Given
        clusterUser.create(kafkaProxy(PROXY_A));
        updateStatusObservedGeneration(clusterUser.create(kafkaService(SERVICE_H)), BOOTSTRAP_SERVERS);
        var cluster = cluster(CLUSTER_BAR, PROXY_A, INGRESS_D, SERVICE_H, null);
        var ingress = clusterIpIngress(INGRESS_D, PROXY_A, TCP);

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
        assertClusterIngressStatusPopulated(clusterBar, ingress, "bar-cluster-ingress-d-bootstrap.%s.svc.cluster.local:9292", Protocol.TCP);
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
        return operator.updateStatus(KafkaProtocolFilter.class, name(filter), fresh -> {
            fresh.setStatus(new KafkaProtocolFilterStatusBuilder().withObservedGeneration(generation(fresh)).build());
            return fresh;
        });
    }

    private KafkaService updateStatusObservedGeneration(KafkaService service, String bootstrapServers) {
        return operator.updateStatus(KafkaService.class, name(service), fresh -> {
            fresh.setStatus(new KafkaServiceStatusBuilder().withObservedGeneration(generation(fresh))
                    .withBootstrapServers(bootstrapServers)
                    .build());
            return fresh;
        });
    }

    private KafkaProxyIngress updateStatusObservedGeneration(KafkaProxyIngress ingress) {
        return operator.updateStatus(KafkaProxyIngress.class, name(ingress), fresh -> {
            fresh.setStatus(new KafkaProxyIngressStatusBuilder().withObservedGeneration(generation(fresh)).build());
            return fresh;
        });
    }

}
