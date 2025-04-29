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
import java.util.function.Consumer;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
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
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.Condition;
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
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.model.ingress.ClusterIPIngressDefinition.serviceName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VirtualKafkaClusterReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
    private static final VirtualKafkaClusterStatusFactory STATUS_FACTORY = new VirtualKafkaClusterStatusFactory(TEST_CLOCK);

    public static final String PROXY_NAME = "my-proxy";
    public static final String NAMESPACE = "my-namespace";
    public static final String SERVER_CERT_NAME = "server-cert";

    // @formatter:off
    public static final VirtualKafkaCluster CLUSTER_NO_FILTERS = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName("foo")
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

    private static final VirtualKafkaCluster CLUSTER_NO_FILTERS_WITH_TLS = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
                .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(SERVER_CERT_NAME)
                        .endCertificateRef()
                    .endTls()
                .build())
            .endSpec()
            .build();
    private static final VirtualKafkaCluster CLUSTER_NO_FILTERS_WITH_TLS_WRONG_RESOURCE_TYPE = new VirtualKafkaClusterBuilder(CLUSTER_NO_FILTERS)
            .editOrNewSpec()
                .withIngresses(new IngressesBuilder(CLUSTER_NO_FILTERS.getSpec().getIngresses().get(0))
                    .withNewTls()
                        .withNewCertificateRef()
                            .withName(SERVER_CERT_NAME)
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
                .withGeneration(101L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaService SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
                .withName("my-kafka")
              .withGeneration(201L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    public static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName("my-ingress")
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
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    public static final Service KUBERNETES_INGRESS_SERVICES = new ServiceBuilder().
            withNewMetadata()
                .withName(serviceName(CLUSTER_NO_FILTERS, INGRESS))
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
            .withName(SERVER_CERT_NAME)
            .withNamespace(NAMESPACE)
            .withGeneration(42L)
            .endMetadata()
            .withType("kubernetes.io/tls")
            .addToData("tls.crt", "value")
            .addToData("tls.key", "value")
            .build();
    private static final Secret NON_KUBE_TLS_CERT_SECRET = new SecretBuilder()
            .withNewMetadata()
            .withName(SERVER_CERT_NAME)
            .withNamespace(NAMESPACE)
            .withGeneration(42L)
            .endMetadata()
            .withType("example.com/nottls")  // unexpected type value
            .addToData("tls.crt", "value")
            .addToData("tls.key", "value")
            .build();

    // @formatter:on

    private VirtualKafkaClusterReconciler virtualKafkaClusterReconciler;

    @BeforeEach
    void setUp() {
        virtualKafkaClusterReconciler = new VirtualKafkaClusterReconciler(Clock.systemUTC(), DependencyResolver.create());
    }

    static List<Arguments> shouldSetResolvedRefs() {
        List<Arguments> result = new ArrayList<>();

        {
            Context<? extends CustomResource<?, ?>> context = mock();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("no filter",
                    CLUSTER_NO_FILTERS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> {
                        conditionList
                                .singleElement()
                                .isResolvedRefsTrue();
                    }));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResource(ConfigMap.class)).thenReturn(Optional.of(buildProxyConfigMapWithPatch(CLUSTER_ONE_FILTER)));
            when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of(FILTER_MY_FILTER));
            result.add(Arguments.argumentSet("one filter",
                    CLUSTER_ONE_FILTER,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> {
                        conditionList
                                .singleElement()
                                .isResolvedRefsTrue();
                    }));
        }

        {

            Context<? extends CustomResource<?, ?>> context = mock();
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
                    (Consumer<ConditionListAssert>) conditionList -> {
                        conditionList
                                .singleElement()
                                .isResolvedRefsTrue();
                    }));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();
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
            Context<? extends CustomResource<?, ?>> context = mock();
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
            Context<? extends CustomResource<?, ?>> context = mock();
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
            Context<? extends CustomResource<?, ?>> context = mock();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(new KafkaServiceBuilder(SERVICE).withNewStatus().addNewCondition()
                    .withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(1L)
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
            Context<? extends CustomResource<?, ?>> context = mock();
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
            Context<? extends CustomResource<?, ?>> context = mock();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(
                    Set.of(new KafkaProxyIngressBuilder(INGRESS).withNewStatus().addNewCondition().withType(Condition.Type.ResolvedRefs)
                            .withStatus(Condition.Status.FALSE)
                            .withLastTransitionTime(Instant.now())
                            .withObservedGeneration(1L)
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
            Context<? extends CustomResource<?, ?>> context = mock();
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
            Context<? extends CustomResource<?, ?>> context = mock();
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            when(context.getSecondaryResources(KafkaProtocolFilter.class)).thenReturn(Set.of(new KafkaProtocolFilterBuilder(FILTER_MY_FILTER).withNewStatus()
                    .addNewCondition()
                    .withType(Condition.Type.ResolvedRefs)
                    .withStatus(Condition.Status.FALSE)
                    .withLastTransitionTime(Instant.now())
                    .withObservedGeneration(1L)
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
            Context<? extends CustomResource<?, ?>> context = mock();
            mockGetSecret(context, Optional.of(KUBE_TLS_CERT_SECRET));
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls",
                    CLUSTER_NO_FILTERS_WITH_TLS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> {
                        conditionList
                                .singleElement()
                                .isResolvedRefsTrue();
                    }));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();

            mockGetSecret(context, Optional.empty());
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls - secret not found",
                    CLUSTER_NO_FILTERS_WITH_TLS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REFS_NOT_FOUND,
                                    "spec.ingresses[].tls.certificateRef: referenced resource not found")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();

            mockGetSecret(context, Optional.of(NON_KUBE_TLS_CERT_SECRET));
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls - wrong secret type",
                    CLUSTER_NO_FILTERS_WITH_TLS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls.certificateRef: referenced secret should have 'type: kubernetes.io/tls'")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();

            mockGetSecret(context, Optional.empty());
            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS_WITH_TLS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster with tls - unsupported resource type",
                    CLUSTER_NO_FILTERS_WITH_TLS_WRONG_RESOURCE_TYPE,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                                    "spec.ingresses[].tls.certificateRef: supports referents: secrets")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();

            when(context.getSecondaryResources(KafkaProxy.class)).thenReturn(Set.of(PROXY));
            when(context.getSecondaryResources(KafkaProxyIngress.class)).thenReturn(Set.of(INGRESS));
            when(context.getSecondaryResources(KafkaService.class)).thenReturn(Set.of(SERVICE));
            result.add(Arguments.argumentSet("cluster defines tls, ingress does not",
                    CLUSTER_NO_FILTERS_WITH_TLS,
                    context,
                    (Consumer<ConditionListAssert>) conditionList -> conditionList
                            .singleElement()
                            .isResolvedRefsFalse(
                                    Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                    "spec.ingresses[].tls: Inconsistent TLS configuration. kafkaproxyingress.kroxylicious.io/my-ingress in namespace 'null' requires the use of TCP but the cluster ingress (my-ingress) defines a tls object.")));
        }

        {
            Context<? extends CustomResource<?, ?>> context = mock();

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
    private static ConfigMap buildProxyConfigMapWithPatch(VirtualKafkaCluster clusterOneFilter) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(clusterOneFilter.getSpec().getProxyRef().getName())
                .endMetadata()
                .withData(new ProxyConfigStateData().addStatusPatchForCluster(
                    ResourcesUtil.name(clusterOneFilter),
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
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(CLUSTER_NO_FILTERS_WITH_TLS);

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
    void mappingToSecretToVirtualKafkaClusterWithTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<VirtualKafkaCluster> mockList = mockListOperation(client, VirtualKafkaCluster.class);
        when(mockList.getItems()).thenReturn(List.of(CLUSTER_NO_FILTERS_WITH_TLS));

        // When
        var mapper = VirtualKafkaClusterReconciler.secretToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(KUBE_TLS_CERT_SECRET);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(CLUSTER_NO_FILTERS_WITH_TLS));
    }

    @Test
    void mappingToSecretToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<VirtualKafkaCluster> mockList = mockListOperation(client, VirtualKafkaCluster.class);
        when(mockList.getItems()).thenReturn(List.of(CLUSTER_NO_FILTERS));

        // When
        var mapper = VirtualKafkaClusterReconciler.secretToVirtualKafkaCluster(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(KUBE_TLS_CERT_SECRET);
        assertThat(primaryResourceIDs).isEmpty();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static void mockGetSecret(Context<? extends CustomResource<?, ?>> context, Optional<Secret> optional) {
        when(context.getSecondaryResource(Secret.class, VirtualKafkaClusterReconciler.SECRETS_EVENT_SOURCE_NAME)).thenReturn(optional);
    }

    private <T extends HasMetadata> KubernetesResourceList<T> mockListOperation(KubernetesClient client, Class<T> clazz) {
        MixedOperation<T, KubernetesResourceList<T>, Resource<T>> mockOperation = mock();
        when(client.resources(clazz)).thenReturn(mockOperation);
        KubernetesResourceList<T> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }

}
