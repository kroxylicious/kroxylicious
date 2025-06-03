/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

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
import org.mockito.Mock;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses.Protocol;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.MetadataAssert;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.OPTIONAL;
import static org.mockito.ArgumentMatchers.any;
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

    private static final VirtualKafkaCluster CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
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

    public static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName("my-ingress")
                .withUid(UUID.randomUUID().toString())
             .withGeneration(301L)
            .endMetadata()
            .withNewSpec()
             .withNewProxyRef().withName(PROXY_NAME).endProxyRef()
            .endSpec()
            .build();
    public static final KafkaProxyIngress INGRESS_WITH_TLS = new KafkaProxyIngressBuilder(INGRESS)
            .editOrNewSpec()
                .withNewClusterIP()
                    .withProtocol(ClusterIP.Protocol.TLS)
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
    public static final Service KUBERNETES_INGRESS_SERVICES = new ServiceBuilder().
            withNewMetadata()
                .withName(name(CLUSTER_NO_FILTERS) + "-" + name(INGRESS))
                .withNamespace(NAMESPACE)
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(CLUSTER_NO_FILTERS)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(INGRESS)).endOwnerReference()
            .endMetadata()
            .withNewSpec()
                .addToPorts(new ServicePortBuilder().withName("port").withPort(9082).build())
            .endSpec()
            .build();

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

    public static final ConfigMap PEM_CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName(TRUST_ANCHOR_CERT_CONFIGMAP_NAME)
                .withNamespace(NAMESPACE)
                .withGeneration(42L)
                .withUid(UUID.randomUUID().toString())
            .endMetadata()
            .addToData("ca-bundle.pem", "value")
            .build();
    // @formatter:on
    @Mock
    private static ManagedWorkflowAndDependentResourceContext workflowContext;
    private VirtualKafkaClusterReconciler virtualKafkaClusterReconciler;

    @BeforeEach
    void setUp() {
        virtualKafkaClusterReconciler = new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create());
        workflowContext = mock(ManagedWorkflowAndDependentResourceContext.class);
    }

    static List<Arguments> shouldSetResolvedRefs() {
        List<Arguments> result = new ArrayList<>();

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("no filter",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_ONE_FILTER)));
            when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of(FILTER_MY_FILTER));
            result.add(Arguments.argumentSet("one filter",
                    CLUSTER_ONE_FILTER,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {

            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(
                    new VirtualKafkaClusterBuilder(CLUSTER_ONE_FILTER).editMetadata().withGeneration(40L).endMetadata().build())));
            when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of(FILTER_MY_FILTER));
            result.add(Arguments.argumentSet("one filter with stale configmap",
                    new VirtualKafkaClusterBuilder(CLUSTER_ONE_FILTER).editOrNewStatus().withObservedGeneration(ResourcesUtil.generation(CLUSTER_NO_FILTERS))
                            .endStatus().build(),
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("proxy not found",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.proxyRef references kafkaproxy.kroxylicious.io/my-proxy in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            result.add(Arguments.argumentSet("service not found",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(
                    Set.of(INGRESS.edit().editSpec().withNewProxyRef().withName("not-my-proxy").endProxyRef().endSpec().build()));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("ingress refers to a different proxy than virtual cluster",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "a spec.ingresses[].ingressRef had an inconsistent or missing proxyRef kafkaproxy.kroxylicious.io/not-my-proxy in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(new KafkaServiceBuilder(SERVICE).withNewStatus().addNewCondition()
                    .withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(SERVICE.getMetadata().getGeneration())
                    .withReason("NO_FILTERS")
                    .withMessage("no filters found")
                    .endCondition().endStatus().build()));
            result.add(Arguments.argumentSet("service has ResolvedRefs=False condition",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "spec.targetKafkaServiceRef references kafkaservice.kroxylicious.io/my-kafka in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("ingress not found",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].ingressRef references kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(
                    Set.of(new KafkaProxyIngressBuilder(INGRESS).withNewStatus().addNewCondition().withType(Condition.Type.ResolvedRefs)
                            .withStatus(Condition.Status.FALSE)
                            .withLastTransitionTime(Instant.now())
                            .withObservedGeneration(INGRESS.getMetadata().getGeneration())
                            .withReason("NO_FILTERS")
                            .withMessage("no filters found")
                            .endCondition().endStatus().build()));

            result.add(Arguments.argumentSet("ingress has ResolvedRefs=False condition",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "spec.ingresses[].ingressRef references kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("filter not found",
                    CLUSTER_ONE_FILTER,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.filterRefs references kafkaprotocolfilter.filter.kroxylicious.io/my-filter in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of(new KafkaProtocolFilterBuilder(FILTER_MY_FILTER).withNewStatus()
                    .addNewCondition()
                    .withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(FILTER_MY_FILTER.getMetadata().getGeneration())
                    .withReason("RESOLVE_FAILURE")
                    .withMessage("failed to resolve")
                    .endCondition().endStatus().build()));

            result.add(Arguments.argumentSet("filter has ResolvedRefs=False condition",
                    CLUSTER_ONE_FILTER,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                                    "spec.filterRefs references kafkaprotocolfilter.filter.kroxylicious.io/my-filter in namespace 'my-namespace'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            mockGetSecret(context, Optional.of(KUBE_TLS_CERT_SECRET));
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls",
                    CLUSTER_TLS_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            mockGetSecret(context, Optional.empty());
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls - server cert secret not found",
                    CLUSTER_TLS_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].tls.certificateRef: referenced resource not found")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            mockGetSecret(context, Optional.of(NON_KUBE_TLS_CERT_SECRET));
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls -  server cert secret wrong type",
                    CLUSTER_TLS_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls.certificateRef: referenced secret should have 'type: kubernetes.io/tls'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            mockGetSecret(context, Optional.empty());
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls - server cert unsupported resource type",
                    CLUSTER_TLS_NO_FILTERS_WITH_SECRET_WRONG_RESOURCE_TYPE,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                                    "spec.ingresses[].tls.certificateRef: supports referents: secrets")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();
            mockGetSecret(context, Optional.of(KUBE_TLS_CERT_SECRET));
            mockGetConfigMap(context, Optional.of(PEM_CONFIG_MAP));
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls with trust anchor",
                    CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsTrue()));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            mockGetSecret(context, Optional.of(KUBE_TLS_CERT_SECRET));
            mockGetConfigMap(context, Optional.empty());
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls - trust anchor cert configmap not found",
                    CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].tls.trustAnchor: referenced resource not found")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster defines tls, ingress does not",
                    CLUSTER_TLS_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls: Inconsistent TLS configuration. kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'null' requires the use of TCP but the cluster ingress (my-ingress) defines a tls object.")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mockContext();

            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster does not define tls, ingress does",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls: Inconsistent TLS configuration. kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'null' requires the use of TLS but the cluster ingress (my-ingress) does not define a tls object.")));
        }

        return result;
    }

    @NonNull
    private static Context<? extends CustomResource<?, ?>> mockContext() {
        Context<? extends CustomResource<?, ?>> context = mock();
        when(context.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);
        return context;
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
    void shouldSetIngressStatusForClusterIPIngress() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());

        Context<VirtualKafkaCluster> context = mock();
        when(context.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);

        when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
        when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_NO_FILTERS)));
        when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
        when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
        when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of());
        when(context.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));

        // when
        var update = reconciler.reconcile(CLUSTER_NO_FILTERS, context);

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
                            assertThat(ingress.getName()).isEqualTo(INGRESS.getMetadata().getName());
                            assertThat(ingress.getBootstrapServer()).isEqualTo("foo-my-ingress.my-namespace.svc.cluster.local:9082");
                            assertThat(ingress.getProtocol()).isEqualTo(Protocol.TCP);
                        }));

    }

    @Test
    void shouldOmitIngressIfKubernetesServiceNotPresent() {
        // given
        var reconciler = new VirtualKafkaClusterReconciler(TEST_CLOCK, DependencyResolver.create());

        Context<VirtualKafkaCluster> context = mock();
        when(context.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);

        when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
        when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_NO_FILTERS)));
        when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
        when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
        when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of());
        when(context.getSecondaryResources(Service.class)).thenReturn(Set.of());

        // when
        var update = reconciler.reconcile(CLUSTER_NO_FILTERS, context);

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
    void canMapFromVirtualKafkaClusterWithServerCertToSecret() {
        // Given
        var mapper = VirtualKafkaClusterReconciler.virtualKafkaClusterToSecret();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(CLUSTER_TLS_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(KUBE_TLS_CERT_SECRET));
    }

    @Test
    void canMapFromVirtualKafkaClusterWithoutServerCertToSecret() {
        // Given
        var mapper = VirtualKafkaClusterReconciler.virtualKafkaClusterToSecret();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(CLUSTER_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromSecretToVirtualKafkaClusterWithTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(CLUSTER_TLS_NO_FILTERS);

        // When
        var mapper = VirtualKafkaClusterReconciler.secretToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(KUBE_TLS_CERT_SECRET);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(CLUSTER_TLS_NO_FILTERS));
    }

    @Test
    void canMapFromSecretToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(CLUSTER_NO_FILTERS);

        // When
        var mapper = VirtualKafkaClusterReconciler.secretToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(KUBE_TLS_CERT_SECRET);
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromVirtualKafkaClusterWithTrustAnchorToConfigMap() {
        // Given
        var mapper = VirtualKafkaClusterReconciler.virtualKafkaClusterToConfigMap();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(PEM_CONFIG_MAP));
    }

    @Test
    void canMapFromVirtualKafkaClusterWithTlsToConfigMap() {
        // Given
        var mapper = VirtualKafkaClusterReconciler.virtualKafkaClusterToConfigMap();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(CLUSTER_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromVirtualKafkaClusterWithoutTrustAnchorToConfigMap() {
        // Given
        var mapper = VirtualKafkaClusterReconciler.virtualKafkaClusterToConfigMap();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(CLUSTER_TLS_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromConfigMapToVirtualKafkaClusterWithTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR);

        // When
        var mapper = VirtualKafkaClusterReconciler.configMapToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(PEM_CONFIG_MAP);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR));
    }

    @Test
    void canMapFromConfigMapToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(CLUSTER_NO_FILTERS);

        // When
        var mapper = VirtualKafkaClusterReconciler.configMapToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(PEM_CONFIG_MAP);
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromConfigMapToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTrustAnchor() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(CLUSTER_TLS_NO_FILTERS);

        // When
        var mapper = VirtualKafkaClusterReconciler.configMapToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(PEM_CONFIG_MAP);
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void ingressSecondaryToPrimaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder()
                        .withNewIngressRef().withName("ingress").endIngressRef().build())
                .endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = VirtualKafkaClusterReconciler.ingressSecondaryToPrimaryMapper(eventSourceContext);
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder().withNewMetadata().withName("ingress").endMetadata().withNewSpec().withNewProxyRef()
                .withName("proxy")
                .endProxyRef().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void filterSecondaryToPrimaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withFilterRefs(new FilterRefBuilder().withName("filter").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = VirtualKafkaClusterReconciler.filterSecondaryToPrimaryMapper(eventSourceContext);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void kafkaServiceSecondaryToPrimaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName("target-kafka").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaService> mapper = VirtualKafkaClusterReconciler.kafkaServiceSecondaryToPrimaryMapper(eventSourceContext);
        KafkaService service = new KafkaServiceBuilder().withNewMetadata().withName("target-kafka").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void filterSecondaryToPrimaryMapperHandlesNullFilterRefs() {
        // given
        VirtualKafkaCluster clusterWithNullFilterRefs = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec().endSpec()
                .build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(clusterWithNullFilterRefs);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = VirtualKafkaClusterReconciler.filterSecondaryToPrimaryMapper(eventSourceContext);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void ingressSecondaryToPrimaryMapperIgnoresIngressWithStaleStatus() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withIngresses(new IngressesBuilder().withNewIngressRef().withName("ingress").endIngressRef().build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProxyIngress> mapper = VirtualKafkaClusterReconciler.ingressSecondaryToPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaProxyIngress ingress = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withName("ingress")
                .withGeneration(23L)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                .withName("proxy")
                .endProxyRef()
                .endSpec()
                .withNewStatus()
                .withObservedGeneration(20L)
                .endStatus()
                .build();
        // @formatter:on

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void filterSecondaryToPrimaryMapperIgnoresFilterWithStaleStatus() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withFilterRefs(new FilterRefBuilder().withName("filter").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = VirtualKafkaClusterReconciler.filterSecondaryToPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaProtocolFilter ingress = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withName("filter")
                .withGeneration(23L)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .withObservedGeneration(20L)
                .endStatus()
                .build();
        // @formatter:on

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void kafkaServiceSecondaryToPrimaryMapperIgnoresServiceWithStaleStatus() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName("target-kafka").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaService> mapper = VirtualKafkaClusterReconciler.kafkaServiceSecondaryToPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaService service = new KafkaServiceBuilder()
                .withNewMetadata()
                .withName("target-kafka")
                .withGeneration(23L)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .withObservedGeneration(20L)
                .endStatus()
                .build();
        // @formatter:on

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void shouldIncludeDownstreamTlsSecretInChecksum() {
        // Given
        Context<VirtualKafkaCluster> context = mock();
        MetadataChecksumGenerator checksumGenerator = mock(MetadataChecksumGenerator.class);
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.of(checksumGenerator));
        when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
        when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS)));
        when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
        when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
        when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of());
        when(context.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(context.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(context.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(KUBE_TLS_CERT_SECRET));
        when(context.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);

        // When
        virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS, context);

        // Then
        verify(checksumGenerator).appendMetadata(KUBE_TLS_CERT_SECRET);
    }

    @Test
    void shouldIncludeDownstreamTlsTrustAnchorInChecksum() {
        // Given
        Context<VirtualKafkaCluster> context = mock();
        MetadataChecksumGenerator checksumGenerator = mock(MetadataChecksumGenerator.class);
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.of(checksumGenerator));
        when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
        when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR)));
        when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
        when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
        when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of());
        when(context.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(context.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(context.getSecondaryResource(ConfigMap.class, "configmaps")).thenReturn(Optional.of(PEM_CONFIG_MAP));
        when(context.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(KUBE_TLS_CERT_SECRET));
        when(context.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(PEM_CONFIG_MAP));
        when(context.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);

        // When
        virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR, context);

        // Then
        verify(checksumGenerator).appendMetadata(PEM_CONFIG_MAP);
    }

    @Test
    void shouldCreateChecksumGeneratorIfNotPresentInReconcilerContext() {
        // Given
        Context<VirtualKafkaCluster> reconcilerContext = mock();
        when(workflowContext.get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)).thenReturn(Optional.empty());
        when(reconcilerContext.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
        when(reconcilerContext.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR)));
        when(reconcilerContext.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
        when(reconcilerContext.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
        when(reconcilerContext.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of());
        when(reconcilerContext.getSecondaryResources(Service.class)).thenReturn(Set.of(KUBERNETES_INGRESS_SERVICES));
        when(reconcilerContext.getSecondaryResource(Secret.class, "secrets")).thenReturn(Optional.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResource(ConfigMap.class, "configmaps")).thenReturn(Optional.of(PEM_CONFIG_MAP));
        when(reconcilerContext.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(KUBE_TLS_CERT_SECRET));
        when(reconcilerContext.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(PEM_CONFIG_MAP));
        when(reconcilerContext.managedWorkflowAndDependentResourceContext()).thenReturn(workflowContext);

        // When
        var actualUpdate = virtualKafkaClusterReconciler.reconcile(CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR, reconcilerContext);

        // Then
        assertThat(actualUpdate)
                .isNotNull()
                .extracting(UpdateControl::getResource).asInstanceOf(OPTIONAL)
                .isPresent()
                .get(InstanceOfAssertFactories.type(VirtualKafkaCluster.class))
                .satisfies(virtualKafkaCluster -> MetadataAssert.assertThat(virtualKafkaCluster)
                        .hasAnnotationSatisfying(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION,
                                (value) -> assertThat(value).isBase64()));
    }

    private static EventSourceContext<VirtualKafkaCluster> mockContextContaining(VirtualKafkaCluster cluster) {
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mockListVirtualClustersOperation(client);
        when(mockList.getItems()).thenReturn(List.of(cluster));
        return eventSourceContext;
    }

    private static KubernetesResourceList<VirtualKafkaCluster> mockListVirtualClustersOperation(KubernetesClient client) {
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> mockOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(mockOperation);
        KubernetesResourceList<VirtualKafkaCluster> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void mockGetSecret(Context<? extends CustomResource<?, ?>> context, Optional<Secret> optional) {
        when(context.getSecondaryResource(Secret.class, VirtualKafkaClusterReconciler.SECRETS_EVENT_SOURCE_NAME)).thenReturn(optional);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void mockGetConfigMap(
                                         Context<? extends CustomResource<?, ?>> context,
                                         Optional<ConfigMap> empty) {
        when(context.getSecondaryResource(ConfigMap.class, VirtualKafkaClusterReconciler.CONFIGMAPS_EVENT_SOURCE_NAME)).thenReturn(empty);
    }

}
