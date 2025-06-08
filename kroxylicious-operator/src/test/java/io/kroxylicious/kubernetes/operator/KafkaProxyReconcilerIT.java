/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.common.CertificateRef;
import io.kroxylicious.kubernetes.api.common.CertificateRefBuilder;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRef;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
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
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatusBuilder;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.kubernetes.operator.assertj.ProxyConfigAssert;
import io.kroxylicious.kubernetes.operator.model.networking.LoadBalancerClusterIngressNetworkingModel;
import io.kroxylicious.kubernetes.operator.model.networking.TlsClusterIPClusterIngressNetworkingModel;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TLS;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.findOnlyResourceNamed;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.generation;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaProxyReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconcilerIT.class);

    private static final String PROXY_A = "proxy-a";
    private static final String PROXY_B = "proxy-b";
    private static final String CLUSTER_FOO_REF = "fooref";
    private static final String FILTER_NAME = "validation";
    private static final String CLUSTER_FOO = "foo";
    private static final String CLUSTER_FOO_CLUSTERIP_INGRESS = "foo-cluster-ip";
    private static final String CLUSTER_FOO_LOADBALANCER_INGRESS = "foo-load-balancer";
    private static final String CLUSTER_FOO_BOOTSTRAP = "my-cluster-kafka-bootstrap.foo.svc.cluster.local:9092";
    private static final String CLUSTER_BAR_REF = "barref";
    private static final String CLUSTER_BAR = "bar";
    private static final String CLUSTER_BAR_CLUSTERIP_INGRESS = "bar-cluster-ip";
    private static final String CLUSTER_BAR_LOADBALANCER_INGRESS = "bar-load-balancer";
    private static final String CLUSTER_BAR_BOOTSTRAP = "my-cluster-kafka-bootstrap.bar.svc.cluster.local:9092";
    private static final String NEW_BOOTSTRAP = "new-bootstrap:9092";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));
    public static final String UPSTREAM_TLS_CERTIFICATE_SECRET_NAME = "upstream-tls-certificate";
    public static final String CA_BUNDLE_CONFIG_MAP_NAME = "ca-bundle";
    public static final String TRUSTED_CAS_PEM = "trusted-cas.pem";
    public static final String PROTOCOL_TLS_V1_3 = "TLSv1.3";
    public static final String TLS_CIPHER_SUITE_AES256GCM_SHA384 = "TLS_AES_256_GCM_SHA384";

    // the initial operator image pull can take a long time and interfere with the tests
    @BeforeAll
    static void preloadOperandImage() {
        OperatorTestUtils.preloadOperandImage();
    }

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole.*.yaml");

    @RegisterExtension
    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect so we can use it as an instance field.
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .withKubernetesClient(rbacHandler.operatorClient())
            .withAdditionalCustomResourceDefinition(VirtualKafkaCluster.class)
            .withAdditionalCustomResourceDefinition(KafkaService.class)
            .withAdditionalCustomResourceDefinition(KafkaProxyIngress.class)
            .withAdditionalCustomResourceDefinition(KafkaProtocolFilter.class)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();
    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() throws Exception {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void testCreate() {
        doCreate();
    }

    @Test
    void testCreateWithKafkaServiceTls() {
        // given
        testActor.create(tlsKeyAndCertSecret(UPSTREAM_TLS_CERTIFICATE_SECRET_NAME));
        testActor.create(new ConfigMapBuilder()
                .withNewMetadata()
                .withName(CA_BUNDLE_CONFIG_MAP_NAME)
                .endMetadata()
                .addToData(TRUSTED_CAS_PEM, "whatever")
                .build());
        KafkaService kafkaService = kafkaServiceWithTls(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP);

        // when
        var created = doCreate(kafkaService);

        // then
        assertProxyConfigContents(created.proxy(), Set
                .of(
                        UPSTREAM_TLS_CERTIFICATE_SECRET_NAME,
                        TRUSTED_CAS_PEM,
                        PROTOCOL_TLS_V1_3,
                        TLS_CIPHER_SUITE_AES256GCM_SHA384),
                Set.of());
        assertDeploymentMountsConfigMap(created.proxy(), CA_BUNDLE_CONFIG_MAP_NAME);
        assertDeploymentMountsSecret(created.proxy(), UPSTREAM_TLS_CERTIFICATE_SECRET_NAME);
    }

    @Test
    void virtualClusterWithClusterIpIngress() {
        // Given
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));

        KafkaService kafkaService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));

        KafkaProxyIngress ingress = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy, TLS)));

        String downstreamCertSecretName = "downstream-tls-certificate";
        Secret tlsCert = testActor.create(tlsKeyAndCertSecret(downstreamCertSecretName));

        Ingresses build = new IngressesBuilder().withIngressRef(toIngressRef(ingress)).withNewTls().withCertificateRef(
                toCertificateRef(tlsCert)).endTls().build();
        VirtualKafkaCluster resource = virtualKafkaCluster(CLUSTER_BAR, proxy, kafkaService, List.of(build), Optional.empty());

        // When
        updateStatusObservedGeneration(testActor.create(resource));

        // Then
        assertProxyConfigContents(proxy, Set.of(downstreamCertSecretName), Set.of());
        assertDeploymentMountsSecret(proxy, downstreamCertSecretName);
    }

    @Test
    void virtualClusterWithClusterIpIngressWithTrustAnchor() {
        // Given
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));

        KafkaService kafkaService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));

        KafkaProxyIngress ingress = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy, TLS)));

        Secret tlsServerCert = testActor.create(tlsKeyAndCertSecret("downstream-tls-certificate"));
        String downstreamTrustAnchorName = "downstream-tls-trust-anchor";
        ConfigMap trustAnchor = testActor.create(trustAnchorConfigMap(downstreamTrustAnchorName, "tls.pem"));

        Ingresses clusterIngress = new IngressesBuilder()
                .withIngressRef(toIngressRef(ingress))
                .withNewTls()
                .withCertificateRef(toCertificateRef(tlsServerCert))
                .withTrustAnchorRef(toTrustAnchorRef(trustAnchor))
                .endTls()
                .build();

        VirtualKafkaCluster resource = virtualKafkaCluster(CLUSTER_BAR, proxy, kafkaService, List.of(clusterIngress), Optional.empty());

        // When
        updateStatusObservedGeneration(testActor.create(resource));

        // Then
        int clientFacingPort = TlsClusterIPClusterIngressNetworkingModel.CLIENT_FACING_PORT;
        int proxyListenPort = ProxyDeploymentDependentResource.SHARED_SNI_PORT;
        assertProxyConfigContents(proxy, Set.of(downstreamTrustAnchorName), Set.of());
        String baseServiceName = name(resource) + "-" + name(ingress);

        String expectedBootstrapHost = baseServiceName + "-bootstrap." + extension.getNamespace() + ".svc.cluster.local";
        String expectedAdvertisedBrokerAddressPattern = baseServiceName + "-$(nodeId)." + extension.getNamespace() + ".svc.cluster.local";
        AWAIT.alias("proxy config - gateway configured for clusterIP SNI ingress").untilAsserted(() -> {
            assertProxyConfigInConfigMap(proxy)
                    .cluster(name(resource))
                    .gateway(name(ingress))
                    .sniHostIdentifiesNode()
                    .hasBootstrapAddress(new HostPort(expectedBootstrapHost, proxyListenPort).toString())
                    .hasAdvertisedBrokerAddressPattern(new HostPort(expectedAdvertisedBrokerAddressPattern, clientFacingPort).toString());
        });

        assertDeploymentMountsConfigMap(proxy, downstreamTrustAnchorName);
        assertSharedSniPortExposedOnProxyDeployment(proxy, proxyListenPort);
        AWAIT.alias("SNI clusterIp services manifested").untilAsserted(() -> {
            assertTlsClusterIpServiceManifested(baseServiceName + "-bootstrap", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName + "-0", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName + "-1", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName + "-2", proxy, clientFacingPort, proxyListenPort);
        });
    }

    @Test
    void virtualClusterWithMultipleClusterIpIngressWithTrustAnchor() {
        // Given
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));

        KafkaService kafkaService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));

        KafkaProxyIngress ingress = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy, TLS)));
        KafkaProxyIngress ingress2 = updateStatusObservedGeneration(testActor.create(clusterIpIngress("another-cluster-ip", proxy, TLS)));

        Secret tlsServerCert = testActor.create(tlsKeyAndCertSecret("downstream-tls-certificate"));
        String downstreamTrustAnchorName = "downstream-tls-trust-anchor";
        ConfigMap trustAnchor = testActor.create(trustAnchorConfigMap(downstreamTrustAnchorName, "tls.pem"));

        Ingresses clusterIngress = new IngressesBuilder()
                .withIngressRef(toIngressRef(ingress))
                .withNewTls()
                .withCertificateRef(toCertificateRef(tlsServerCert))
                .withTrustAnchorRef(toTrustAnchorRef(trustAnchor))
                .endTls()
                .build();

        Ingresses clusterIngress2 = new IngressesBuilder()
                .withIngressRef(toIngressRef(ingress2))
                .withNewTls()
                .withCertificateRef(toCertificateRef(tlsServerCert))
                .withTrustAnchorRef(toTrustAnchorRef(trustAnchor))
                .endTls()
                .build();

        VirtualKafkaCluster resource = virtualKafkaCluster(CLUSTER_BAR, proxy, kafkaService, List.of(clusterIngress, clusterIngress2), Optional.empty());

        // When
        updateStatusObservedGeneration(testActor.create(resource));

        // Then
        int clientFacingPort = TlsClusterIPClusterIngressNetworkingModel.CLIENT_FACING_PORT;
        int proxyListenPort = ProxyDeploymentDependentResource.SHARED_SNI_PORT;
        assertProxyConfigContents(proxy, Set.of(downstreamTrustAnchorName), Set.of());
        String baseServiceName = name(resource) + "-" + name(ingress);
        String baseServiceName2 = name(resource) + "-" + name(ingress2);

        assertProxyGatewayConfiguredForTlsClusterIP(baseServiceName, proxy, resource, ingress, proxyListenPort, clientFacingPort);
        assertProxyGatewayConfiguredForTlsClusterIP(baseServiceName2, proxy, resource, ingress2, proxyListenPort, clientFacingPort);

        assertDeploymentMountsConfigMap(proxy, downstreamTrustAnchorName);
        assertSharedSniPortExposedOnProxyDeployment(proxy, proxyListenPort);
        AWAIT.alias("SNI clusterIp services manifested").untilAsserted(() -> {
            assertTlsClusterIpServiceManifested(baseServiceName + "-bootstrap", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName + "-0", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName + "-1", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName + "-2", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName2 + "-bootstrap", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName2 + "-0", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName2 + "-1", proxy, clientFacingPort, proxyListenPort);
            assertTlsClusterIpServiceManifested(baseServiceName2 + "-2", proxy, clientFacingPort, proxyListenPort);
        });
    }

    private void assertProxyGatewayConfiguredForTlsClusterIP(String baseServiceName, KafkaProxy proxy, VirtualKafkaCluster resource, KafkaProxyIngress ingress,
                                                             int proxyListenPort, int clientFacingPort) {
        String expectedBootstrapHost = baseServiceName + "-bootstrap." + extension.getNamespace() + ".svc.cluster.local";
        String expectedAdvertisedBrokerAddressPattern = baseServiceName + "-$(nodeId)." + extension.getNamespace() + ".svc.cluster.local";
        AWAIT.alias("proxy config - gateway configured for clusterIP SNI ingress").untilAsserted(() -> {
            assertProxyConfigInConfigMap(proxy)
                    .cluster(name(resource))
                    .gateway(name(ingress))
                    .sniHostIdentifiesNode()
                    .hasBootstrapAddress(new HostPort(expectedBootstrapHost, proxyListenPort).toString())
                    .hasAdvertisedBrokerAddressPattern(new HostPort(expectedAdvertisedBrokerAddressPattern, clientFacingPort).toString());
        });
    }

    private void assertTlsClusterIpServiceManifested(String serviceName, KafkaProxy proxy, int clientFacingPort, int proxyListenPort) {
        var service = testActor.get(Service.class, serviceName);
        assertThat(service).isNotNull()
                .describedAs(
                        "Expect Service '" + serviceName + " to exist")
                .extracting(svc -> svc.getSpec().getSelector())
                .describedAs("Service's selector should select proxy pods")
                .isEqualTo(ProxyDeploymentDependentResource.podLabels(proxy));
        assertThat(service.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(service.getSpec().getPorts()).singleElement().satisfies(onlyPort -> {
            assertThat(onlyPort.getProtocol()).isEqualTo("TCP");
            assertThat(onlyPort.getPort()).isEqualTo(clientFacingPort);
            assertThat(onlyPort.getTargetPort()).isEqualTo(new IntOrString(proxyListenPort));
        });
    }

    @Test
    void virtualClusterWithLoadBalancerIngressWithTrustAnchor() {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaService kafkaService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));

        String loadbalancerBootstrap = "bootstrap.kafka";
        String loadbalancerBrokerAddressPattern = "broker-$(nodeId).kafka";
        KafkaProxyIngress loadBalancerIngress = updateStatusObservedGeneration(
                testActor.create(loadBalancerIngress(CLUSTER_BAR_LOADBALANCER_INGRESS, proxy, loadbalancerBootstrap,
                        loadbalancerBrokerAddressPattern)));

        Secret tlsServerCert = testActor.create(tlsKeyAndCertSecret("downstream-tls-certificate"));
        String downstreamTrustAnchorName = "downstream-tls-trust-anchor";
        ConfigMap trustAnchor = testActor.create(trustAnchorConfigMap(downstreamTrustAnchorName, "tls.pem"));

        Ingresses clusterIngress = new IngressesBuilder()
                .withIngressRef(toIngressRef(loadBalancerIngress))
                .withNewTls()
                .withCertificateRef(toCertificateRef(tlsServerCert))
                .withTrustAnchorRef(toTrustAnchorRef(trustAnchor))
                .endTls()
                .build();
        VirtualKafkaCluster cluster = virtualKafkaCluster(CLUSTER_BAR, proxy, kafkaService, List.of(clusterIngress), Optional.empty());

        // when
        updateStatusObservedGeneration(testActor.create(cluster));

        int clientFacingPort = LoadBalancerClusterIngressNetworkingModel.DEFAULT_LOADBALANCER_PORT;
        int proxyListenPort = ProxyDeploymentDependentResource.SHARED_SNI_PORT;

        AWAIT.alias("shared sni service manifested").untilAsserted(() -> {
            String serviceName = name(proxy) + "-sni";
            var service = testActor.get(Service.class, serviceName);
            assertThat(service).isNotNull()
                    .describedAs(
                            "Expect shared SNI Service for proxy '" + name(proxy) + " to exist")
                    .extracting(svc -> svc.getSpec().getSelector())
                    .describedAs("Service's selector should select proxy pods")
                    .isEqualTo(ProxyDeploymentDependentResource.podLabels(proxy));
            assertThat(service.getSpec().getType()).isEqualTo("LoadBalancer");
            // cannot use equality because the ServicePort has a random nodePort assigned to it
            assertThat(service.getSpec().getPorts()).singleElement().satisfies(onlyPort -> {
                String expectedName = "sni-" + clientFacingPort;
                assertThat(onlyPort.getName()).isEqualTo(expectedName);
                assertThat(onlyPort.getProtocol()).isEqualTo("TCP");
                assertThat(onlyPort.getPort()).isEqualTo(clientFacingPort);
                assertThat(onlyPort.getTargetPort()).isEqualTo(new IntOrString(proxyListenPort));
            });
        });

        assertSharedSniPortExposedOnProxyDeployment(proxy, proxyListenPort);

        AWAIT.alias("proxy config - gateway configured for SNI loadbalancer ingress").untilAsserted(() -> {
            assertProxyConfigInConfigMap(proxy)
                    .cluster(name(cluster))
                    .gateway(name(loadBalancerIngress))
                    .sniHostIdentifiesNode()
                    .hasBootstrapAddress(new HostPort(loadbalancerBootstrap, proxyListenPort).toString())
                    .hasAdvertisedBrokerAddressPattern(new HostPort(loadbalancerBrokerAddressPattern, clientFacingPort).toString());
        });
    }

    private void assertSharedSniPortExposedOnProxyDeployment(KafkaProxy proxy, int proxyListenPort) {
        AWAIT.alias("proxy deployment exposes shared sni port").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeploymentDependentResource.deploymentName(proxy));
            assertThat(deployment).isNotNull();
            List<Container> containers = deployment.getSpec().getTemplate().getSpec().getContainers();
            assertThat(containers).hasSize(1).singleElement().satisfies(container -> {
                assertThat(container.getName()).isEqualTo("proxy");
                ContainerPort metricsPort = new ContainerPortBuilder().withContainerPort(9190).withName("management").withProtocol("TCP").build();
                ContainerPort bootstrapContainerPort = createContainerPort(proxyListenPort, "shared-sni-port");
                assertThat(container.getPorts())
                        .containsExactlyInAnyOrder(
                                metricsPort,
                                bootstrapContainerPort);
            });
        });
    }

    @Test
    void virtualClusterWithLoadBalancerAndClusterIpIngress() {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaService kafkaService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));

        KafkaProxyIngress loadBalancerIngress = updateStatusObservedGeneration(testActor.create(loadBalancerIngress(CLUSTER_BAR_LOADBALANCER_INGRESS, proxy,
                "bootstrap.kafka",
                "broker-$(nodeId).kafka")));

        Secret loadBalancerTlsServerCert = testActor.create(tlsKeyAndCertSecret("loadbalancer-tls-certificate"));

        Ingresses lbIngress = new IngressesBuilder()
                .withIngressRef(toIngressRef(loadBalancerIngress))
                .withNewTls()
                .withCertificateRef(toCertificateRef(loadBalancerTlsServerCert))
                .endTls()
                .build();

        Secret clusterIpTlsServerCert = testActor.create(tlsKeyAndCertSecret("clusterip-tls-certificate"));

        KafkaProxyIngress clusterIpIngress = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy, TLS)));

        Ingresses cipIngress = new IngressesBuilder()
                .withIngressRef(toIngressRef(clusterIpIngress))
                .withNewTls()
                .withCertificateRef(toCertificateRef(clusterIpTlsServerCert))
                .endTls()
                .build();

        VirtualKafkaCluster cluster = virtualKafkaCluster(CLUSTER_BAR, proxy, kafkaService, List.of(lbIngress, cipIngress), Optional.empty());

        // when
        updateStatusObservedGeneration(testActor.create(cluster));

        // then
        AWAIT.alias("services manifested").untilAsserted(() -> {
            String sharedSniServiceName = name(proxy) + "-sni";
            String clusterIpServiceName = CLUSTER_BAR + "-" + clusterIpIngress.getMetadata().getName() + "-bootstrap";
            var services = testActor.resources(Service.class).list().getItems();
            assertThat(services)
                    .extracting(service -> service.getMetadata().getName())
                    .contains(sharedSniServiceName, clusterIpServiceName);
        });

        AWAIT.alias("proxy config - gateway configured for both ingress types").untilAsserted(() -> {
            assertProxyConfigInConfigMap(proxy)
                    .cluster(name(cluster))
                    .extracting(VirtualCluster::gateways, list(VirtualClusterGateway.class))
                    .extracting(VirtualClusterGateway::name)
                    .containsExactly(name(loadBalancerIngress), name(clusterIpIngress));
        });
    }

    @Test
    void twoVirtualClusterUsingLoadBalancer() {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaService kafkaService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));

        KafkaProxyIngress loadBalancerIngressFoo = updateStatusObservedGeneration(testActor.create(loadBalancerIngress(CLUSTER_FOO_LOADBALANCER_INGRESS, proxy,
                "bootstrap.foo.kafka",
                "broker-$(nodeId).foo.kafka")));

        KafkaProxyIngress loadBalancerIngressBar = updateStatusObservedGeneration(testActor.create(loadBalancerIngress(CLUSTER_BAR_LOADBALANCER_INGRESS, proxy,
                "bootstrap.bar.kafka",
                "broker-$(nodeId).bar.kafka")));

        Secret loadBalancerTlsServerCertFoo = testActor.create(tlsKeyAndCertSecret("loadbalancer-tls-certificate-foo"));
        Secret loadBalancerTlsServerCertBar = testActor.create(tlsKeyAndCertSecret("loadbalancer-tls-certificate-bar"));

        Ingresses lbIngressFoo = new IngressesBuilder()
                .withIngressRef(toIngressRef(loadBalancerIngressFoo))
                .withNewTls()
                .withCertificateRef(toCertificateRef(loadBalancerTlsServerCertFoo))
                .endTls()
                .build();

        Ingresses lbIngressBar = new IngressesBuilder()
                .withIngressRef(toIngressRef(loadBalancerIngressBar))
                .withNewTls()
                .withCertificateRef(toCertificateRef(loadBalancerTlsServerCertBar))
                .endTls()
                .build();

        VirtualKafkaCluster fooCluster = testActor.create(virtualKafkaCluster(CLUSTER_FOO, proxy, kafkaService, List.of(lbIngressFoo), Optional.empty()));
        VirtualKafkaCluster barCluster = testActor.create(virtualKafkaCluster(CLUSTER_BAR, proxy, kafkaService, List.of(lbIngressBar), Optional.empty()));

        // when
        List.of(fooCluster, barCluster).forEach(this::updateStatusObservedGeneration);

        // then
        AWAIT.alias("shared sni service manifested").untilAsserted(() -> {
            String sharedSniServiceName = name(proxy) + "-sni";
            var services = testActor.resources(Service.class).list().getItems();
            assertThat(services)
                    .extracting(service -> service.getMetadata().getName())
                    .containsExactly(sharedSniServiceName);
        });

        AWAIT.alias("proxy config - each virtual cluster configured with correct ingress").untilAsserted(() -> {
            assertProxyConfigInConfigMap(proxy)
                    .cluster(name(fooCluster))
                    .gateway(name(loadBalancerIngressFoo))
                    .sniHostIdentifiesNode()
                    .hasBootstrapAddress("bootstrap.foo.kafka:" + ProxyDeploymentDependentResource.SHARED_SNI_PORT);

            assertProxyConfigInConfigMap(proxy)
                    .cluster(name(barCluster))
                    .gateway(name(loadBalancerIngressBar))
                    .sniHostIdentifiesNode()
                    .hasBootstrapAddress("bootstrap.bar.kafka:" + ProxyDeploymentDependentResource.SHARED_SNI_PORT);
        });
    }

    @Test
    void clusterIpIngressUsesDeclaredNodeIdsOfService() {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaProtocolFilter filter = testActor.create(filter(FILTER_NAME));
        filter = updateStatusObservedGeneration(filter);
        KafkaService barService = testActor.create(new KafkaServiceBuilder().withNewMetadata().withName(CLUSTER_BAR_REF).endMetadata()
                .withNewSpec()
                .withBootstrapServers(CLUSTER_BAR_BOOTSTRAP)
                .withNodeIdRanges(createNodeIdRanges("brokers", 3L, 4L), createNodeIdRanges("more-brokers", 10L, 10L))
                .endSpec().build());
        barService = updateStatusObservedGeneration(barService);
        KafkaProxyIngress ingressBar = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy, TCP)));
        VirtualKafkaCluster clusterBar = testActor.create(virtualKafkaCluster(CLUSTER_BAR, proxy, barService,
                List.of(new IngressesBuilder().withIngressRef(toIngressRef(ingressBar)).build()), Optional.of(filter)));
        updateStatusObservedGeneration(clusterBar);
        clusterBar.setStatus(new VirtualKafkaClusterStatusBuilder().withObservedGeneration(generation(clusterBar)).build());

        int expectedBootstrapPort = 9292;
        AWAIT.alias("service configured with a port per node id from the KafkaService, plus a bootstrap port").untilAsserted(() -> {
            String clusterName = name(clusterBar);
            String ingressName = name(ingressBar);
            String serviceName = clusterName + "-" + ingressName + "-bootstrap";
            var service = testActor.get(Service.class, serviceName);
            assertThat(service).isNotNull()
                    .describedAs(
                            "Expect Service for cluster '" + clusterName + "' and ingress '" + ingressName + "' to still exist")
                    .extracting(svc -> svc.getSpec().getSelector())
                    .describedAs("Service's selector should select proxy pods")
                    .isEqualTo(ProxyDeploymentDependentResource.podLabels(proxy));
            ServicePort bootstrapServicePort = clusterIpServicePort(CLUSTER_BAR, expectedBootstrapPort);
            ServicePort node0ServicePort = clusterIpServicePort(CLUSTER_BAR, expectedBootstrapPort + 1);
            ServicePort node1ServicePort = clusterIpServicePort(CLUSTER_BAR, expectedBootstrapPort + 2);
            ServicePort node2ServicePort = clusterIpServicePort(CLUSTER_BAR, expectedBootstrapPort + 3);
            assertThat(service.getSpec().getPorts()).containsExactly(bootstrapServicePort, node0ServicePort, node1ServicePort, node2ServicePort);
        });

        AWAIT.alias("deployment pod template configured to expose ports").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeploymentDependentResource.deploymentName(proxy));
            assertThat(deployment).isNotNull();
            List<Container> containers = deployment.getSpec().getTemplate().getSpec().getContainers();
            assertThat(containers).hasSize(1).singleElement().satisfies(container -> {
                assertThat(container.getName()).isEqualTo("proxy");
                ContainerPort metricsPort = new ContainerPortBuilder().withContainerPort(9190).withName("management").withProtocol("TCP").build();
                ContainerPort bootstrapContainerPort = createBootstrapContainerPort(expectedBootstrapPort);
                ContainerPort node0ContainerPort = createNodeContainerPort(expectedBootstrapPort + 1);
                ContainerPort node1ContainerPort = createNodeContainerPort(expectedBootstrapPort + 2);
                ContainerPort node2ContainerPort = createNodeContainerPort(expectedBootstrapPort + 3);
                assertThat(container.getPorts())
                        .containsExactlyInAnyOrder(
                                metricsPort,
                                bootstrapContainerPort,
                                node0ContainerPort,
                                node1ContainerPort,
                                node2ContainerPort);
            });
        });

        AWAIT.alias("proxy config - gateway configured with node id ranges from KafkaService").untilAsserted(() -> {
            ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNodeGatewayAssert = assertProxyConfigInConfigMap(proxy)
                    .cluster(clusterBar.getMetadata().getName())
                    .gateway(name(ingressBar))
                    .portIdentifiesNode()
                    .hasBootstrapAddress(new HostPort("localhost", expectedBootstrapPort))
                    .hasNullNodeStartPort();
            portIdentifiesNodeGatewayAssert
                    .namedRange("brokers")
                    .hasStart(3)
                    .hasEnd(4);
            portIdentifiesNodeGatewayAssert
                    .namedRange("more-brokers")
                    .hasStart(10)
                    .hasEnd(10);
        });
    }

    private static @NonNull ContainerPort createNodeContainerPort(int node1Port) {
        return createContainerPort(node1Port, node1Port + "-node");
    }

    private static @NonNull ContainerPort createBootstrapContainerPort(int bootstrapPort) {
        return createContainerPort(bootstrapPort, bootstrapPort + "-bootstrap");
    }

    private static @NonNull ServicePort clusterIpServicePort(String clusterName, int port) {
        return createServicePort(clusterName + "-" + port, port, port);
    }

    private static ContainerPort createContainerPort(int port, String name) {
        return new ContainerPortBuilder().withContainerPort(port).withName(name).withProtocol("TCP").build();
    }

    private static NodeIdRanges createNodeIdRanges(String brokers, long start, long end) {
        return new NodeIdRangesBuilder().withName(brokers).withStart(start).withEnd(end).build();
    }

    private static ServicePort createServicePort(String name, int port, int targetPort) {
        return new ServicePortBuilder().withName(name).withPort(port).withProtocol("TCP").withTargetPort(new IntOrString(targetPort)).build();
    }

    private record CreatedResources(KafkaProxy proxy, Set<VirtualKafkaCluster> clusters, Set<KafkaService> services, Set<KafkaProxyIngress> ingresses) {

        VirtualKafkaCluster cluster(String name) {
            return findOnlyResourceNamed(name, clusters).orElseThrow();
        }

        KafkaService kafkaService(String name) {
            return findOnlyResourceNamed(name, services).orElseThrow();
        }

        KafkaProxyIngress ingress(String name) {
            return findOnlyResourceNamed(name, ingresses).orElseThrow();
        }

    }

    CreatedResources doCreate() {
        KafkaService kafkaService = kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP);
        return doCreate(kafkaService);
    }

    CreatedResources doCreate(KafkaService kafkaService) {
        KafkaProxy proxy = testActor.create(kafkaProxy(PROXY_A));
        KafkaProtocolFilter filter = testActor.create(filter(FILTER_NAME));
        filter = updateStatusObservedGeneration(filter);
        KafkaService barService = testActor.create(kafkaService);
        barService = updateStatusObservedGeneration(barService);
        KafkaProxyIngress ingressBar = testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxy, TCP));
        ingressBar = updateStatusObservedGeneration(ingressBar);
        Set<KafkaService> kafkaServices = Set.of(barService);
        VirtualKafkaCluster clusterBar = testActor.create(virtualKafkaCluster(CLUSTER_BAR, proxy, barService,
                List.of(new IngressesBuilder().withIngressRef(toIngressRef(ingressBar)).build()), Optional.of(filter)));
        clusterBar = updateStatusObservedGeneration(clusterBar);
        Set<VirtualKafkaCluster> clusters = Set.of(clusterBar);
        assertProxyConfigContents(proxy, Set.of(CLUSTER_BAR_BOOTSTRAP, filter.getSpec().getType()), Set.of());
        assertDefaultVirtualClusterGatewayConfigured(proxy, ingressBar, clusterBar);
        assertDeploymentMountsConfigMap(proxy, ProxyConfigDependentResource.configMapName(proxy));
        assertDeploymentBecomesReady(proxy);
        assertServiceTargetsProxyInstances(proxy, clusterBar, ingressBar);
        return new CreatedResources(proxy, clusters, kafkaServices, Set.of(ingressBar));
    }

    private void assertDefaultVirtualClusterGatewayConfigured(KafkaProxy proxy, KafkaProxyIngress ingressBar, VirtualKafkaCluster clusterBar) {
        AWAIT.alias("gateway configured as expected").untilAsserted(() -> {
            assertProxyConfigInConfigMap(proxy).cluster(clusterBar.getMetadata().getName())
                    .gateway(name(ingressBar))
                    .portIdentifiesNode()
                    .hasBootstrapAddress(new HostPort("localhost", 9292))
                    .hasNullNodeStartPort()
                    .namedRange("default")
                    .hasStart(0)
                    .hasEnd(2);
        });
    }

    private ProxyConfigAssert assertProxyConfigInConfigMap(KafkaProxy proxy) {
        var configMap = testActor.get(ConfigMap.class, ProxyConfigDependentResource.configMapName(proxy));
        return assertThat(configMap)
                .isNotNull()
                .extracting(ConfigMap::getData, InstanceOfAssertFactories.map(String.class, String.class))
                .containsKey(ProxyConfigDependentResource.CONFIG_YAML_KEY)
                .extracting(map -> parse(map.get(ProxyConfigDependentResource.CONFIG_YAML_KEY)), OperatorAssertions.CONFIGURATION);
    }

    private static Configuration parse(String content) {
        try {
            // use base object mapper to avoid our plugin loading code, so that we don't try to load filter plugins etc.
            return ConfigParser.createBaseObjectMapper().readValue(content, Configuration.class);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaProxyIngress clusterIpIngress(String ingressName, KafkaProxy proxy, ClusterIP.Protocol protocol) {
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
                        .withName(name(proxy))
                    .endProxyRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    private KafkaProxyIngress loadBalancerIngress(String ingressName,
                                                  KafkaProxy proxy,
                                                  String bootstrapAddress,
                                                  String advertisedBrokerAddressPattern) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(ingressName)
                .endMetadata()
                .withNewSpec()
                    .withNewLoadBalancer()
                        .withBootstrapAddress(bootstrapAddress)
                        .withAdvertisedBrokerAddressPattern(advertisedBrokerAddressPattern)
                    .endLoadBalancer()
                    .withNewProxyRef()
                        .withName(name(proxy))
                    .endProxyRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    private void assertDeploymentBecomesReady(KafkaProxy proxy) {
        // wait longer for initial operator image download
        AWAIT.alias("Deployment as expected").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeploymentDependentResource.deploymentName(proxy));
            assertThat(deployment)
                    .describedAs("All deployment replicas should become ready")
                    .returns(true, Readiness::isDeploymentReady);
        });
    }

    private void assertServiceTargetsProxyInstances(KafkaProxy proxy, VirtualKafkaCluster cluster, KafkaProxyIngress ingress) {
        AWAIT.alias("cluster Services as expected").untilAsserted(() -> {
            String clusterName = name(cluster);
            String ingressName = name(ingress);
            String serviceName = clusterName + "-" + ingressName + "-bootstrap";
            var service = testActor.get(Service.class, serviceName);
            assertThat(service).isNotNull()
                    .describedAs(
                            "Expect Service for cluster '" + clusterName + "' and ingress '" + ingressName + "' to still exist")
                    .extracting(svc -> svc.getSpec().getSelector())
                    .describedAs("Service's selector should select proxy pods")
                    .isEqualTo(ProxyDeploymentDependentResource.podLabels(proxy));
            assertThat(service.getSpec().getPorts().stream().count()).describedAs("number of ports").isEqualTo(4);
        });
    }

    private void assertDeploymentMountsSecret(KafkaProxy proxy, String secretName) {
        assertDeploymentMounts(proxy,
                Volume::getSecret,
                secretVoumeSource -> secretVoumeSource.getSecretName().equals(secretName));
    }

    private void assertDeploymentMountsConfigMap(KafkaProxy proxy, String configMapName) {
        assertDeploymentMounts(proxy,
                Volume::getConfigMap,
                configMapVoumeSource -> configMapVoumeSource.getName().equals(configMapName));
    }

    private <T> void assertDeploymentMounts(KafkaProxy proxy, Function<Volume, T> volumeSourceExtractor, Predicate<T> volumeSourcePredicate) {
        AWAIT.alias("Deployment as expected").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeploymentDependentResource.deploymentName(proxy));
            assertThat(deployment).isNotNull()
                    .extracting(dep -> dep.getSpec().getTemplate().getSpec().getVolumes(), InstanceOfAssertFactories.list(Volume.class))
                    .describedAs("Deployment template should mount the proxy config configmap")
                    .filteredOn(volume -> volumeSourceExtractor.apply(volume) != null)
                    .map(volumeSourceExtractor)
                    .anyMatch(volumeSourcePredicate::test);
        });
    }

    private void assertProxyConfigContents(KafkaProxy cr, Set<String> contains, Set<String> notContains) {
        AWAIT.alias("Config as expected").untilAsserted(() -> {
            AbstractStringAssert<?> proxyConfig = assertThatProxyConfigFor(cr);
            if (!contains.isEmpty()) {
                proxyConfig.contains(contains);
            }
            if (!notContains.isEmpty()) {
                proxyConfig.doesNotContain(notContains);
            }
        });
    }

    @Test
    void testDelete() {
        var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        testActor.delete(proxy);

        AWAIT.alias("ConfigMap was deleted").untilAsserted(() -> {
            var configMap = testActor.get(ConfigMap.class, ProxyConfigDependentResource.configMapName(proxy));
            assertThat(configMap).isNull();
        });
        AWAIT.alias("Deployment was deleted").untilAsserted(() -> {
            var deployment = testActor.get(Deployment.class, ProxyDeploymentDependentResource.deploymentName(proxy));
            assertThat(deployment).isNull();
        });
        AWAIT.alias("Services were deleted").untilAsserted(() -> {
            for (var cluster : createdResources.clusters) {
                var service = testActor.get(Service.class, ClusterServiceDependentResource.serviceName(cluster));
                assertThat(service).isNull();
            }
        });
    }

    @Test
    void testUpdateVirtualClusterTargetBootstrap() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        var kafkaService = createdResources.kafkaService(CLUSTER_BAR_REF).edit().editSpec().withBootstrapServers(NEW_BOOTSTRAP).endSpec().build();
        kafkaService = testActor.replace(kafkaService);
        updateStatusObservedGeneration(kafkaService);

        assertDeploymentBecomesReady(proxy);
        AWAIT.untilAsserted(() -> {
            assertThatProxyConfigFor(proxy)
                    .doesNotContain(CLUSTER_BAR_BOOTSTRAP)
                    .contains(NEW_BOOTSTRAP);
        });

        assertServiceTargetsProxyInstances(proxy, createdResources.cluster(CLUSTER_BAR), createdResources.ingress(CLUSTER_BAR_CLUSTERIP_INGRESS));
    }

    @Test
    void testUpdateVirtualClusterClusterRef() {
        // given
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;

        String newClusterRefName = "new-cluster-ref";
        updateStatusObservedGeneration(testActor.create(kafkaService(newClusterRefName, NEW_BOOTSTRAP)));

        KafkaServiceRef newClusterRef = new KafkaServiceRefBuilder().withName(newClusterRefName).build();
        var cluster = createdResources.cluster(CLUSTER_BAR).edit().editSpec().withTargetKafkaServiceRef(newClusterRef).endSpec().build();

        // when
        cluster = testActor.replace(cluster);
        updateStatusObservedGeneration(cluster);

        // then
        assertDeploymentBecomesReady(proxy);
        AWAIT.untilAsserted(() -> assertThatProxyConfigFor(proxy)
                .doesNotContain(CLUSTER_BAR_BOOTSTRAP)
                .contains(NEW_BOOTSTRAP));

        assertServiceTargetsProxyInstances(proxy, createdResources.cluster(CLUSTER_BAR), createdResources.ingress(CLUSTER_BAR_CLUSTERIP_INGRESS));
    }

    @Test
    void testDeleteVirtualCluster() {
        final var createdResources = doCreate();
        KafkaProxy proxy = createdResources.proxy;
        testActor.delete(createdResources.cluster(CLUSTER_BAR));

        AWAIT.untilAsserted(() -> {
            var configMap = testActor.get(ConfigMap.class, ProxyConfigDependentResource.configMapName(proxy));
            assertThat(configMap)
                    .describedAs("Expect ConfigMap for cluster 'bar' to have been deleted")
                    .isNull();

            var service = testActor.get(Service.class, CLUSTER_BAR);
            assertThat(service)
                    .describedAs("Expect Service for cluster 'bar' to have been deleted")
                    .isNull();
        });
    }

    @Test
    void moveVirtualKafkaClusterToAnotherKafkaProxy() {
        // given
        KafkaProxy proxyA = testActor.create(kafkaProxy(PROXY_A));
        KafkaProxy proxyB = testActor.create(kafkaProxy(PROXY_B));
        KafkaProxyIngress ingressFoo = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_FOO_CLUSTERIP_INGRESS, proxyA, TCP)));
        KafkaProxyIngress ingressBar = updateStatusObservedGeneration(testActor.create(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxyB, TCP)));

        KafkaService fooService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP)));
        KafkaService barService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_BAR_REF, CLUSTER_BAR_BOOTSTRAP)));
        KafkaProtocolFilter filter = updateStatusObservedGeneration(testActor.create(filter(FILTER_NAME)));

        VirtualKafkaCluster fooCluster = testActor.create(virtualKafkaCluster(CLUSTER_FOO, proxyA, fooService,
                List.of(new IngressesBuilder().withIngressRef(toIngressRef(ingressFoo)).build()), Optional.of(filter)));
        updateStatusObservedGeneration(fooCluster);
        VirtualKafkaCluster barCluster = testActor.create(virtualKafkaCluster(CLUSTER_BAR, proxyB, barService,
                List.of(new IngressesBuilder().withIngressRef(toIngressRef(ingressBar)).build()), Optional.of(filter)));
        updateStatusObservedGeneration(barCluster);

        assertProxyConfigContents(proxyA, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of());
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_BAR_BOOTSTRAP), Set.of());
        assertServiceTargetsProxyInstances(proxyA, fooCluster, ingressFoo);
        assertServiceTargetsProxyInstances(proxyB, barCluster, ingressBar);

        // must swap ingresses so both proxy instances have a single port-per-broker ingress
        updateStatusObservedGeneration(testActor.replace(clusterIpIngress(CLUSTER_FOO_CLUSTERIP_INGRESS, proxyB, TCP)));
        updateStatusObservedGeneration(testActor.replace(clusterIpIngress(CLUSTER_BAR_CLUSTERIP_INGRESS, proxyA, TCP)));
        var updatedFooCluster = new VirtualKafkaClusterBuilder(testActor.get(VirtualKafkaCluster.class, CLUSTER_FOO)).editSpec().editProxyRef().withName(name(proxyB))
                .endProxyRef().endSpec()
                .build();
        updatedFooCluster = testActor.replace(updatedFooCluster);
        updateStatusObservedGeneration(updatedFooCluster);
        var updatedBarCluster = new VirtualKafkaClusterBuilder(testActor.get(VirtualKafkaCluster.class, CLUSTER_BAR)).editSpec().editProxyRef().withName(name(proxyA))
                .endProxyRef().endSpec()
                .build();
        updatedBarCluster = testActor.replace(updatedBarCluster);
        updateStatusObservedGeneration(updatedBarCluster);

        // then
        assertDeploymentBecomesReady(proxyA);
        assertDeploymentBecomesReady(proxyB);
        assertProxyConfigContents(proxyA, Set.of(CLUSTER_BAR_BOOTSTRAP), Set.of(CLUSTER_FOO_BOOTSTRAP));
        assertProxyConfigContents(proxyB, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of(CLUSTER_BAR_BOOTSTRAP));
        assertServiceTargetsProxyInstances(proxyA, barCluster, ingressBar);
        assertServiceTargetsProxyInstances(proxyB, fooCluster, ingressFoo);
    }

    // we want to ensure that if a dangling ref is created, for example if a KafkaIngress is deleted, and then
    // that KafkaIngress is created, the proxy springs back into life.
    @Test
    void deleteAndRestoreADependency() {
        // given
        KafkaProxy proxyA = testActor.create(kafkaProxy(PROXY_A));

        KafkaProxyIngress ingress = clusterIpIngress(CLUSTER_FOO_CLUSTERIP_INGRESS, proxyA, TCP);
        KafkaProxyIngress ingressFoo = updateStatusObservedGeneration(testActor.create(ingress.edit().build()));

        KafkaService fooService = updateStatusObservedGeneration(testActor.create(kafkaService(CLUSTER_FOO_REF, CLUSTER_FOO_BOOTSTRAP)));
        KafkaProtocolFilter filter = updateStatusObservedGeneration(testActor.create(filter(FILTER_NAME)));

        VirtualKafkaCluster fooCluster = updateStatusObservedGeneration(
                testActor.create(virtualKafkaCluster(CLUSTER_FOO, proxyA, fooService, List.of(new IngressesBuilder().withIngressRef(toIngressRef(ingressFoo)).build()),
                        Optional.of(filter))));

        assertProxyConfigContents(proxyA, Set.of(CLUSTER_FOO_BOOTSTRAP), Set.of());
        assertServiceTargetsProxyInstances(proxyA, fooCluster, ingressFoo);

        // when
        testActor.delete(ingressFoo);

        // then
        assertDeploymentIsRemoved(proxyA);

        // and when
        // we need an ingress without uid/resourceVersion in its metadata, so we clone an unadulterated ingress
        updateStatusObservedGeneration(testActor.create(ingress.edit().build()));

        // and then
        assertDeploymentBecomesReady(proxyA);
    }

    // the KafkaProxyReconciler only operates on Clusters that have been reconciled, ie metadata.status == status.observedGeneration
    private VirtualKafkaCluster updateStatusObservedGeneration(VirtualKafkaCluster clusterBar) {
        clusterBar.setStatus(new VirtualKafkaClusterStatusBuilder().withObservedGeneration(generation(clusterBar)).build());
        return testActor.patchStatus(clusterBar);
    }

    // the KafkaProxyReconciler only operates on KafkaProtocolFilters that have been reconciled, ie metadata.status == status.observedGeneration
    private KafkaProtocolFilter updateStatusObservedGeneration(KafkaProtocolFilter filter) {
        filter.setStatus(new KafkaProtocolFilterStatusBuilder().withObservedGeneration(generation(filter)).build());
        return testActor.patchStatus(filter);
    }

    // the KafkaProxyReconciler only operates on KafkaServices that have been reconciled, ie metadata.status == status.observedGeneration
    private KafkaService updateStatusObservedGeneration(KafkaService service) {
        service.setStatus(new KafkaServiceStatusBuilder().withObservedGeneration(generation(service)).build());
        return testActor.patchStatus(service);
    }

    // the KafkaProxyReconciler only operates on KafkaServices that have been reconciled, ie metadata.status == status.observedGeneration
    private KafkaProxyIngress updateStatusObservedGeneration(KafkaProxyIngress ingress) {
        ingress.setStatus(new KafkaProxyIngressStatusBuilder().withObservedGeneration(generation(ingress)).build());
        return testActor.patchStatus(ingress);
    }

    private void assertDeploymentIsRemoved(KafkaProxy proxy) {
        // wait longer for initial operator image download
        AWAIT.alias("Deployment is removed").untilAsserted(() -> {
            assertThat(testActor.get(Deployment.class, ProxyDeploymentDependentResource.deploymentName(proxy))).isNull();
        });
    }

    private AbstractStringAssert<?> assertThatProxyConfigFor(KafkaProxy proxy) {
        var configMap = testActor.get(ConfigMap.class, ProxyConfigDependentResource.configMapName(proxy));
        return assertThat(configMap)
                .isNotNull()
                .extracting(ConfigMap::getData, InstanceOfAssertFactories.map(String.class, String.class))
                .containsKey(ProxyConfigDependentResource.CONFIG_YAML_KEY)
                .extracting(map -> map.get(ProxyConfigDependentResource.CONFIG_YAML_KEY), InstanceOfAssertFactories.STRING);
    }

    private static VirtualKafkaCluster virtualKafkaCluster(String clusterName, KafkaProxy proxy, KafkaService service,
                                                           List<Ingresses> ingresses, Optional<KafkaProtocolFilter> filter) {
        var filterRefs = filter.map(f -> new FilterRefBuilder().withName(name(f)).build()).stream().toList();
        var serviceRef = new KafkaServiceRefBuilder().withName(name(service)).build();

        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                .endMetadata()
                .withNewSpec()
                    .withTargetKafkaServiceRef(serviceRef)
                    .withNewProxyRef()
                        .withName(name(proxy))
                    .endProxyRef()
                    .withIngresses(ingresses)
                    .withFilterRefs(filterRefs)
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaServiceWithTls(String serviceName, String clusterBootstrap) {
        // @formatter:off
        return new KafkaServiceBuilder(kafkaService(serviceName, clusterBootstrap))
                .editSpec()
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(UPSTREAM_TLS_CERTIFICATE_SECRET_NAME)
                        .endCertificateRef()
                        .withNewTrustAnchorRef()
                            .withNewRef()
                                .withName(CA_BUNDLE_CONFIG_MAP_NAME)
                            .endRef()
                            .withKey(TRUSTED_CAS_PEM)
                        .endTrustAnchorRef()
                        .withNewProtocols()
                            .withAllow(PROTOCOL_TLS_V1_3)
                        .endProtocols()
                        .withNewCipherSuites()
                            .withAllow(TLS_CIPHER_SUITE_AES256GCM_SHA384)
                        .endCipherSuites()
                    .endTls()
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaService(String serviceName, String clusterBootstrap) {
        return new KafkaServiceBuilder().withNewMetadata().withName(serviceName).endMetadata()
                .withNewSpec()
                .withBootstrapServers(clusterBootstrap)
                .endSpec().build();
    }

    private static KafkaProtocolFilter filter(String name) {
        return new KafkaProtocolFilterBuilder().withNewMetadata().withName(name).endMetadata()
                .withNewSpec().withType("RecordValidation").withConfigTemplate(Map.of("rules", List.of(Map.of("allowNulls", false))))
                .endSpec().build();
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

    private ConfigMap trustAnchorConfigMap(String name, String filename) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .addToData(filename, "whatever")
                .addToData("key", filename)
                .build();
    }

    private static IngressRef toIngressRef(HasMetadata resource) {
        return new IngressRefBuilder().withName(resource.getMetadata().getName()).build();
    }

    private CertificateRef toCertificateRef(HasMetadata resource) {
        return new CertificateRefBuilder().withName(resource.getMetadata().getName()).build();
    }

    private TrustAnchorRef toTrustAnchorRef(ConfigMap trustAnchor) {
        return new TrustAnchorRefBuilder()
                .withNewRef()
                .withName(trustAnchor.getMetadata().getName())
                .endRef()
                .withKey(trustAnchor.getData().get("key"))
                .build();
    }

}
