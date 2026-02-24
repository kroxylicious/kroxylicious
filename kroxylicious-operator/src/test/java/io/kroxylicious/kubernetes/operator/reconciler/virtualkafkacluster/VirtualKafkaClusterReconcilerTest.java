/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.Protocol;
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
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ProxyConfigStateData;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.MetadataAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.OPTIONAL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VirtualKafkaClusterReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    private static final VirtualKafkaClusterStatusFactory STATUS_FACTORY = new VirtualKafkaClusterStatusFactory(TEST_CLOCK);

    public static final String PROXY_NAME = "my-proxy";
    public static final String NAMESPACE = "my-namespace";
    public static final String SERVER_CERT_SECRET_NAME = "server-cert";
    public static final String TRUST_ANCHOR_CERT_CONFIGMAP_NAME = "trust-anchor-cert";
    public static final String TRUST_ANCHOR_CERT_SECRET_NAME = "trust-anchor-secret";

    // @formatter:off
    public static final VirtualKafkaCluster CLUSTER_NO_FILTERS = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName("foo")
                .withUid(UUID.randomUUID().toString())
                .withNamespace(NAMESPACE)
                .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
                .withNewProxyRef()
                    .withName(PROXY_NAME)
                .endProxyRef()
                .addNewIngress()
                    .withNewIngressRef()
                        .withName("my-ingress")
                    .endIngressRef()
                .endIngress()
                .withNewTargetKafkaServiceRef()
                    .withName("my-kafka")
                .endTargetKafkaServiceRef()
            .endSpec()
            .build();

    private static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
                .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(SERVER_CERT_SECRET_NAME)
                        .endCertificateRef()
                    .endTls()
                .build())
            .endSpec()
            .build();

    private static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
                .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(SERVER_CERT_SECRET_NAME)
                        .endCertificateRef()
                        .withNewTrustAnchorRef()
                        .withNewRef()
                            .withName(TRUST_ANCHOR_CERT_CONFIGMAP_NAME)
                        .endRef()
                        .withKey("ca-bundle.pem")
                        .endTrustAnchorRef()
                    .endTls()
                .build())
            .endSpec()
            .build();

    private static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
                .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(SERVER_CERT_SECRET_NAME)
                        .endCertificateRef()
                        .withNewTrustAnchorRef()
                            .withNewRef()
                                .withKind("Secret")
                                .withName(TRUST_ANCHOR_CERT_SECRET_NAME)
                            .endRef()
                        .withKey("ca-bundle.pem")
                        .endTrustAnchorRef()
                    .endTls()
                    .build())
            .endSpec()
            .build();

    private static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_SECRET_WRONG_RESOURCE_TYPE = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
                .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(SERVER_CERT_SECRET_NAME)
                            .withKind("ConfigMap")
                        .endCertificateRef()
                    .endTls()
                .build())
            .endSpec()
            .build();
    public static final VirtualKafkaCluster CLUSTER_ONE_FILTER = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editSpec()
                .addNewFilterRef()
                    .withName("my-filter")
                .endFilterRef()
            .endSpec()
            .build();

    public static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName(PROXY_NAME)
                .withUid(UUID.randomUUID().toString())
                .withGeneration(101L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaService SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
                .withName("my-kafka")
                .withUid(UUID.randomUUID().toString())
              .withGeneration(201L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaProxyIngress CLUSTERIP_INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName("my-ingress")
                .withUid(UUID.randomUUID().toString())
             .withGeneration(301L)
            .endMetadata()
            .withNewSpec()
             .withNewProxyRef().withName(PROXY_NAME).endProxyRef()
             .withNewClusterIP().withProtocol(Protocol.TCP).endClusterIP()
            .endSpec()
            .build();

    public static final KafkaProxyIngress LOADBALANCER_INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName("my-ingress")
                .withUid(UUID.randomUUID().toString())
             .withGeneration(301L)
            .endMetadata()
            .withNewSpec()
             .withNewProxyRef().withName(PROXY_NAME).endProxyRef()
             .withNewLoadBalancer()
            .withBootstrapAddress("bootstrap.kafka")
            .withAdvertisedBrokerAddressPattern("broker-$(nodeId).kafka")
            .endLoadBalancer()
            .endSpec()
            .build();

    public static final KafkaProxyIngress INGRESS_WITH_TLS = new KafkaProxyIngressBuilder(CLUSTERIP_INGRESS)
            .editOrNewSpec()
                .withNewClusterIP()
                    .withProtocol(Protocol.TLS)
                .endClusterIP()
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_MY_FILTER = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
                .withName("my-filter")
                .withGeneration(401L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();


    public static final String CLUSTERIP_BOOTSTRAP = "clusterip-bootstrap:1234";

    public static final Service KUBERNETES_INGRESS_SERVICES;
    static {
        var serviceBuilderMetadataNested = new ServiceBuilder().withNewMetadata();
        Annotations.ClusterIngressBootstrapServers bootstrap = new Annotations.ClusterIngressBootstrapServers(name(CLUSTER_NO_FILTERS), name(LOADBALANCER_INGRESS), CLUSTERIP_BOOTSTRAP);
        Annotations.annotateWithBootstrapServers(serviceBuilderMetadataNested, Set.of(bootstrap));
        KUBERNETES_INGRESS_SERVICES=serviceBuilderMetadataNested
                    .withName(name(CLUSTER_NO_FILTERS) + "-" + name(CLUSTERIP_INGRESS))
                    .withNamespace(NAMESPACE)
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(CLUSTER_NO_FILTERS)).endOwnerReference()
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(CLUSTERIP_INGRESS)).endOwnerReference()
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .addToPorts(new ServicePortBuilder().withName("port").withPort(9082).build())
                .endSpec()
                .build();
    }

    public static final String LOADBALANCER_BOOTSTRAP = "loadbalancer.bootstrap:123";

    public static final Service KUBERNETES_SHARED_SNI_SERVICE;
    public static final String SHARED_SNI_LOADBALANCER_IP = "10.13.11.22";
    public static final String SHARED_SNI_LOADBALANCER_HOSTNAME = "sni-hostname";

    static {
        var metadataBuilder = new ServiceBuilder().withNewMetadata();
        Annotations.ClusterIngressBootstrapServers bootstrap = new Annotations.ClusterIngressBootstrapServers(name(CLUSTER_NO_FILTERS), name(LOADBALANCER_INGRESS), LOADBALANCER_BOOTSTRAP);
        Annotations.annotateWithBootstrapServers(metadataBuilder, Set.of(bootstrap));
        LoadBalancerIngress loadBalancerIngress = new LoadBalancerIngressBuilder()
                .withHostname(SHARED_SNI_LOADBALANCER_HOSTNAME)
                .withIp(SHARED_SNI_LOADBALANCER_IP)
                .build();
        KUBERNETES_SHARED_SNI_SERVICE=metadataBuilder
                .withName(PROXY_NAME + "-sni")
                .withNamespace(NAMESPACE)
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(PROXY)).endOwnerReference()
            .endMetadata()
            .withNewSpec()
                .withType("LoadBalancer")
                .addToPorts(new ServicePortBuilder().withName("port").withPort(9082).build())
            .endSpec()
                .withStatus(new ServiceStatusBuilder().withNewLoadBalancer().withIngress(loadBalancerIngress).endLoadBalancer().build())
            .build();
    }

    private static final Secret KUBE_TLS_CERT_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(SERVER_CERT_SECRET_NAME)
                .withNamespace(NAMESPACE)
                .withUid(UUID.randomUUID().toString())
                .withGeneration(42L)
            .endMetadata()
            .withType("kubernetes.io/tls")
            .addToData("tls.crt", "value")
            .addToData("tls.key", "value")
            .build();

    private static final Secret NON_KUBE_TLS_CERT_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(SERVER_CERT_SECRET_NAME)
                .withNamespace(NAMESPACE)
                .withUid(UUID.randomUUID().toString())
                .withGeneration(42L)
            .endMetadata()
            .withType("example.com/nottls")  // unexpected type value
            .addToData("tls.crt", "value")
            .addToData("tls.key", "value")
            .build();

    public static final ConfigMap TRUST_ANCHOR_PEM_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(TRUST_ANCHOR_CERT_CONFIGMAP_NAME)
                .withNamespace(NAMESPACE)
                .withGeneration(42L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();

    public static final Secret TRUST_ANCHOR_PEM_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(TRUST_ANCHOR_CERT_SECRET_NAME)
                .withNamespace(NAMESPACE)
                .withGeneration(42L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();
    // @formatter:on

    private static final ConfigMap NO_FILTERS_CONFIG_MAP = buildProxyConfigMapWithPatch(CLUSTER_NO_FILTERS);

    private static final ManagedWorkflowAndDependentResourceContext workflowContext = mock(ManagedWorkflowAndDependentResourceContext.class);
    private VirtualKafkaClusterReconciler virtualKafkaClusterReconciler;

    @BeforeEach
    void setUp() {
        virtualKafkaClusterReconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());

    }

    static List<Arguments> shouldSetResolvedRefs() {
        List<Arguments> result = new ArrayList<>();

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("no filter",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            ConfigMap proxyConfigMap = buildProxyConfigMapWithPatch(CLUSTER_ONE_FILTER);
            Set<KafkaProtocolFilter> filters = Set.of(FILTER_MY_FILTER);

            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, proxyConfigMap, filters);

            result.add(Arguments.argumentSet("one filter",
                    CLUSTER_ONE_FILTER,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {

            ConfigMap proxyConfigMap = buildProxyConfigMapWithPatch(
                    new VirtualKafkaClusterBuilder(CLUSTER_ONE_FILTER).editMetadata().withGeneration(40L).endMetadata().build());
            Set<KafkaProtocolFilter> filters = Set.of(FILTER_MY_FILTER);

            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, proxyConfigMap, filters);

            result.add(Arguments.argumentSet("one filter with stale configmap",
                    new VirtualKafkaClusterBuilder(CLUSTER_ONE_FILTER).editOrNewStatus().withObservedGeneration(ResourcesUtil.generation(CLUSTER_NO_FILTERS))
                            .endStatus().build(),
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(null, CLUSTERIP_INGRESS, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("proxy not found",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.proxyRef references kafkaproxy.kroxylicious.io/my-proxy in namespace 'my-namespace'")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, null, null, Set.of());
            result.add(Arguments.argumentSet("service not found",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka in namespace 'my-namespace'")));
        }

        {

            KafkaProxyIngress ingress = CLUSTERIP_INGRESS.edit().editSpec().withNewProxyRef().withName("not-my-proxy").endProxyRef().endSpec().build();
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, ingress, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("ingress refers to a different proxy than virtual cluster",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "a spec.ingresses[].ingressRef had an inconsistent or missing proxyRef kafkaproxy.kroxylicious.io/not-my-proxy in namespace 'my-namespace'")));
        }

        {
            KafkaService service = new KafkaServiceBuilder(SERVICE).withNewStatus().addNewCondition()
                    .withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(SERVICE.getMetadata().getGeneration())
                    .withReason("NO_FILTERS")
                    .withMessage("no filters found")
                    .endCondition().endStatus().build();
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, service, null, Set.of());

            result.add(Arguments.argumentSet("service has ResolvedRefs=False condition",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka in namespace 'my-namespace'")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, null, SERVICE, null, Set.of());
            result.add(Arguments.argumentSet("ingress not found",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].ingressRef references kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'my-namespace'")));
        }

        {

            KafkaProxyIngress ingress = new KafkaProxyIngressBuilder(CLUSTERIP_INGRESS).withNewStatus().addNewCondition().withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(CLUSTERIP_INGRESS.getMetadata().getGeneration())
                    .withReason("NO_FILTERS")
                    .withMessage("no filters found")
                    .endCondition().endStatus().build();

            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, ingress, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("ingress has ResolvedRefs=False condition",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "spec.ingresses[].ingressRef references kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'my-namespace'")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("filter not found",
                    CLUSTER_ONE_FILTER,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.filterRefs references kafkaprotocolfilter.kroxylicious.io/my-filter in namespace 'my-namespace'")));
        }

        {
            Set<KafkaProtocolFilter> filters = Set.of(new KafkaProtocolFilterBuilder(FILTER_MY_FILTER).withNewStatus()
                    .addNewCondition()
                    .withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(FILTER_MY_FILTER.getMetadata().getGeneration())
                    .withReason("RESOLVE_FAILURE")
                    .withMessage("failed to resolve")
                    .endCondition().endStatus().build());
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, null, filters);

            result.add(Arguments.argumentSet("filter has ResolvedRefs=False condition",
                    CLUSTER_ONE_FILTER,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "spec.filterRefs references kafkaprotocolfilter.kroxylicious.io/my-filter in namespace 'my-namespace'")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());
            mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));

            result.add(Arguments.argumentSet("cluster with tls",
                    CLUSTER_TLS_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.empty());

            result.add(Arguments.argumentSet("cluster with tls - server cert secret not found",
                    CLUSTER_TLS_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].tls.certificateRef: referenced secret not found")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.of(NON_KUBE_TLS_CERT_SECRET));

            result.add(Arguments.argumentSet("cluster with tls -  server cert secret wrong type",
                    CLUSTER_TLS_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls.certificateRef: referenced secret should have 'type: kubernetes.io/tls'")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.empty());

            result.add(Arguments.argumentSet("cluster with tls - server cert unsupported resource type",
                    CLUSTER_TLS_NO_FILTERS_WITH_SECRET_WRONG_RESOURCE_TYPE,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                                    "spec.ingresses[].tls.certificateRef: supports referents: secrets")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));
            mockGetConfigMapTrustAnchorRef(reconcilerContext, Optional.of(TRUST_ANCHOR_PEM_CONFIG_MAP));

            result.add(Arguments.argumentSet("cluster with tls with configmap trust anchor",
                    CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));
            mockGetSecretTrustAnchorRef(reconcilerContext, Optional.of(TRUST_ANCHOR_PEM_SECRET));

            result.add(Arguments.argumentSet("cluster with tls with secret trust anchor",
                    CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));
            mockGetConfigMapTrustAnchorRef(reconcilerContext, Optional.empty());

            result.add(Arguments.argumentSet("cluster with tls - trust anchor cert configmap not found",
                    CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].tls.trustAnchor: referenced configmap not found")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));
            mockGetSecretTrustAnchorRef(reconcilerContext, Optional.empty());

            result.add(Arguments.argumentSet("cluster with tls - trust anchor cert secret not found",
                    CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].tls.trustAnchor: referenced secret not found")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("cluster defines tls, ingress does not",
                    CLUSTER_TLS_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls: Inconsistent TLS configuration. kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'null' requires the use of TCP but the cluster ingress (my-ingress) defines a tls object.")));
        }

        {
            Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, null, Set.of());

            result.add(Arguments.argumentSet("cluster does not define tls, ingress does",
                    CLUSTER_NO_FILTERS,
                    reconcilerContext,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls: Inconsistent TLS configuration. kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'null' requires the use of TLS but the cluster ingress (my-ingress) does not define a tls object.")));
        }

        return result;
    }

    private static Context<VirtualKafkaCluster> mockReconcilerContext(@Nullable KafkaProxy proxy, @Nullable KafkaProxyIngress ingress, @Nullable KafkaService service,
                                                                      @Nullable ConfigMap proxyConfigMap,
                                                                      Set<KafkaProtocolFilter> filters) {
        Context<VirtualKafkaCluster> reconcilerContext = mock();
        when(reconcilerContext.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);

        when(reconcilerContext.getSecondaryResources(KafkaProxy.class)).thenReturn(setOfOrEmpty(proxy));
        when(reconcilerContext.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(setOfOrEmpty(ingress));
        when(reconcilerContext.getSecondaryResources(KafkaService.class)).thenReturn(setOfOrEmpty(service));
        when(reconcilerContext.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.ofNullable(proxyConfigMap));
        when(reconcilerContext.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(filters);

        return reconcilerContext;
    }

    @NonNull
    private static <T> Set<T> setOfOrEmpty(@Nullable T item) {
        return item != null ? Set.of(item) : Set.of();
    }

    @NonNull
    private static ConfigMap buildProxyConfigMapWithPatch(VirtualKafkaCluster clusterOneFilter) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterOneFilter.getSpec().getProxyRef().getName())
                .endMetadata()
                .withData(new ProxyConfigStateData().addStatusPatchForCluster(
                    name(clusterOneFilter),
                    STATUS_FACTORY.newTrueConditionStatusPatch(clusterOneFilter, Condition.Type.ResolvedRefs, "")).build())
                .build();
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource
    void shouldSetResolvedRefs(VirtualKafkaCluster kafkaService, Context<VirtualKafkaCluster> context, Consumer<ConditionListAssert> asserter) {

        // When
        final UpdateControl<VirtualKafkaCluster> updateControl = virtualKafkaClusterReconciler.reconcile(kafkaService, context);

        // Then
        assertThat(updateControl).isNotNull();
        assertThat(updateControl.getResource()).isPresent();
        var c = VirtualKafkaClusterStatusAssert.assertThat(updateControl.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .conditionList();
        asserter.accept(c);
    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());

        Context<VirtualKafkaCluster> context = mock();

        // when
        var update = reconciler.updateErrorStatus(CLUSTER_NO_FILTERS, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER_NO_FILTERS)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER_NO_FILTERS)
                .isResolvedRefsUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(TEST_CLOCK.instant());

    }

    @Test
    void shouldSetIngressStatusForLoadBalancerIngress() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, LOADBALANCER_INGRESS, SERVICE, NO_FILTERS_CONFIG_MAP,
                Set.of());
        mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_SHARED_SNI_SERVICE));

        // when
        var update = reconciler.reconcile(CLUSTER_TLS_NO_FILTERS, reconcilerContext);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource())
                .isPresent()
                .get()
                .satisfies(r -> assertThat(r.getStatus())
                        .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                        .singleElement()
                        .satisfies(ingress -> {
                            assertThat(ingress.getName()).isEqualTo(CLUSTERIP_INGRESS.getMetadata().getName());
                            assertThat(ingress.getBootstrapServer()).isEqualTo(LOADBALANCER_BOOTSTRAP);
                            assertThat(ingress.getProtocol()).isEqualTo(Protocol.TLS);
                            assertThat(ingress.getLoadBalancerIngressPoints()).singleElement().satisfies(ingressPoint -> {
                                assertThat(ingressPoint.getHostname()).isEqualTo(SHARED_SNI_LOADBALANCER_HOSTNAME);
                                assertThat(ingressPoint.getIp()).isEqualTo(SHARED_SNI_LOADBALANCER_IP);
                            });
                        }));

    }

    @Test
    void shoulNotSetIngressStatusForLoadBalancerIngressWithNoStatus() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, LOADBALANCER_INGRESS, SERVICE, NO_FILTERS_CONFIG_MAP,
                Set.of());
        mockGetSecret(reconcilerContext, Optional.of(KUBE_TLS_CERT_SECRET));
        Service loadbalancerServiceWithNoStatus = KUBERNETES_SHARED_SNI_SERVICE.edit().withStatus(null).build();
        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(loadbalancerServiceWithNoStatus));

        // when
        var update = reconciler.reconcile(CLUSTER_TLS_NO_FILTERS, reconcilerContext);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource())
                .isPresent()
                .get()
                .satisfies(r -> assertThat(r.getStatus())
                        .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                        .singleElement()
                        .satisfies(ingress -> {
                            assertThat(ingress.getName()).isEqualTo(CLUSTERIP_INGRESS.getMetadata().getName());
                            assertThat(ingress.getBootstrapServer()).isEqualTo(LOADBALANCER_BOOTSTRAP);
                            assertThat(ingress.getProtocol()).isEqualTo(Protocol.TLS);
                            assertThat(ingress.getLoadBalancerIngressPoints()).isNull();
                        }));

    }

    @Test
    void shouldSetIngressStatusForClusterIPIngress() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, NO_FILTERS_CONFIG_MAP,
                Set.of());

        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));

        // when
        var update = reconciler.reconcile(CLUSTER_NO_FILTERS, reconcilerContext);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource())
                .isPresent()
                .get()
                .satisfies(r -> assertThat(r.getStatus())
                        .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                        .singleElement()
                        .satisfies(ingress -> {
                            assertThat(ingress.getName()).isEqualTo(CLUSTERIP_INGRESS.getMetadata().getName());
                            assertThat(ingress.getBootstrapServer()).isEqualTo(CLUSTERIP_BOOTSTRAP);
                            assertThat(ingress.getProtocol()).isEqualTo(Protocol.TCP);
                        }));

    }

    @Test
    void shouldOmitIngressIfKubernetesServiceNotPresent() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());

        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, CLUSTERIP_INGRESS, SERVICE, NO_FILTERS_CONFIG_MAP,
                Set.of());
        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of());

        // when
        var update = reconciler.reconcile(CLUSTER_NO_FILTERS, reconcilerContext);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource())
                .isPresent()
                .get()
                .satisfies(r -> assertThat(r.getStatus())
                        .extracting(VirtualKafkaClusterStatus::getIngresses, InstanceOfAssertFactories.list(Ingresses.class))
                        .isEmpty());
    }

    @Test
    void shouldIncludeDownstreamTlsSecretInChecksum() {
        // Given
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS),
                Set.of());

        MetadataChecksumGenerator checksumGenerator = mock(MetadataChecksumGenerator.class);
        when(checksumGenerator.encode()).thenReturn("==BaSe64");
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.of(checksumGenerator));

        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(reconcilerContext.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(KUBE_TLS_CERT_SECRET));

        // When
        virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS, reconcilerContext);

        // Then
        verify(checksumGenerator).appendMetadata(KUBE_TLS_CERT_SECRET);
    }

    @Test
    void shouldIncludeDownstreamTlsTrustAnchorInChecksumConfigMapTrustAnchorRef() {
        // Given
        ConfigMap proxyConfigMap = buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR);
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, proxyConfigMap, Set.of());
        MetadataChecksumGenerator checksumGenerator = mock(MetadataChecksumGenerator.class);
        when(checksumGenerator.encode()).thenReturn("==BaSe64");
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.of(checksumGenerator));

        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(reconcilerContext.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResource(ConfigMap.class, "configmapsTrustAnchorRef")).thenReturn(Optional.of(TRUST_ANCHOR_PEM_CONFIG_MAP));
        when(reconcilerContext.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(TRUST_ANCHOR_PEM_CONFIG_MAP));

        // When
        virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR, reconcilerContext);

        // Then
        verify(checksumGenerator).appendMetadata(TRUST_ANCHOR_PEM_CONFIG_MAP);
    }

    @Test
    void shouldIncludeDownstreamTlsTrustAnchorInChecksumSecretTrustAnchorRef() {
        // Given
        ConfigMap proxyConfigMap = buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR);
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, proxyConfigMap, Set.of());
        MetadataChecksumGenerator checksumGenerator = mock(MetadataChecksumGenerator.class);
        when(checksumGenerator.encode()).thenReturn("==BaSe64");
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.of(checksumGenerator));

        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(reconcilerContext.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResource(Secret.class, "secretsTrustAnchorRef")).thenReturn(Optional.of(TRUST_ANCHOR_PEM_SECRET));

        // using `thenAnswer` since this mock is called twice and if we use `thenReturn` it returns a closed stream secondary time
        when(reconcilerContext.getSecondaryResourcesAsStream(Secret.class))
                .thenAnswer(invocation -> Stream.of(KUBE_TLS_CERT_SECRET, TRUST_ANCHOR_PEM_SECRET));
        // When
        virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR, reconcilerContext);

        // Then
        verify(checksumGenerator).appendMetadata(TRUST_ANCHOR_PEM_SECRET);
    }

    @Test
    void shouldCreateChecksumGeneratorIfNotPresentInReconcilerContext() {
        // Given
        ConfigMap proxyConfigMap = buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR);
        Context<VirtualKafkaCluster> reconcilerContext = mockReconcilerContext(PROXY, INGRESS_WITH_TLS, SERVICE, proxyConfigMap, Set.of());
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.empty());

        when(reconcilerContext.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(proxyConfigMap));
        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(reconcilerContext.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResource(ConfigMap.class, "configmapsTrustAnchorRef")).thenReturn(Optional.of(TRUST_ANCHOR_PEM_CONFIG_MAP));
        when(reconcilerContext.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(TRUST_ANCHOR_PEM_CONFIG_MAP));

        // When
        var actualUpdate = virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS_WITH_CONFIGMAP_TRUST_ANCHOR, reconcilerContext);

        // Then
        assertThat(actualUpdate)
                .isNotNull()
                .extracting(UpdateControl::getResource).asInstanceOf(OPTIONAL)
                .isPresent()
                .get(InstanceOfAssertFactories.type(VirtualKafkaCluster.class))
                .satisfies(virtualKafkaCluster -> MetadataAssert.assertThat(virtualKafkaCluster)
                        .hasAnnotationSatisfying(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY,
                                value -> assertThat(value).isBase64()));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void mockGetSecret(Context<? extends CustomResource<?, ?>> context, Optional<Secret> optional) {
        when(context.getSecondaryResource(Secret.class, VirtualKafkaClusterReconciler.SECRETS_EVENT_SOURCE_NAME)).thenReturn(optional);
    }

    private static void mockGetSecretTrustAnchorRef(Context<? extends CustomResource<?, ?>> context, Optional<Secret> optional) {
        when(context.getSecondaryResource(Secret.class, VirtualKafkaClusterReconciler.SECRET_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME)).thenReturn(optional);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void mockGetConfigMapTrustAnchorRef(
                                                       Context<? extends CustomResource<?, ?>> context,
                                                       Optional<ConfigMap> empty) {
        when(context.getSecondaryResource(ConfigMap.class, VirtualKafkaClusterReconciler.CONFIG_MAPS_TRUST_ANCHOR_REF_EVENT_SOURCE_NAME)).thenReturn(empty);
    }

}
