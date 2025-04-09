/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedReferences;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.fabric8.kubernetes.api.model.HasMetadata.getKind;
import static io.kroxylicious.kubernetes.operator.ProxyConfigStateDependentResource.CONFIG_STATE_CONFIG_MAP_SUFFIX;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

/**
 * Reconciles a {@link VirtualKafkaCluster} by checking whether the resources
 * referenced by the {@code spec.proxyRef.name}, {@code spec.targetClusterRef.name},
 * {@code spec.ingressRefs[].name} and {@code spec.filterRefs[].name} actually exist,
 * setting a {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.
 */
public final class VirtualKafkaClusterReconciler implements
        Reconciler<VirtualKafkaCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconciler.class);
    public static final String PROXY_EVENT_SOURCE_NAME = "proxy";
    public static final String PROXY_CONFIG_STATE_SOURCE_NAME = "proxy-config-state";
    public static final String SERVICES_EVENT_SOURCE_NAME = "services";
    public static final String INGRESSES_EVENT_SOURCE_NAME = "ingresses";
    public static final String FILTERS_EVENT_SOURCE_NAME = "filters";
    public static final String TRANSITIVELY_REFERENCED_RESOURCES_NOT_FOUND = "TransitivelyReferencedResourcesNotFound";
    public static final String REFERENCED_RESOURCES_NOT_FOUND = "ReferencedResourcesNotFound";
    private static final String KAFKA_PROXY_INGRESS_KIND = getKind(KafkaProxyIngress.class);
    private static final String KAFKA_PROXY_KIND = getKind(KafkaProxy.class);
    private static final String KAFKA_SERVICE_KIND = getKind(KafkaService.class);
    private static final String KAFKA_PROTOCOL_FILTER_KIND = getKind(KafkaProtocolFilter.class);

    private final VirtualKafkaClusterStatusFactory statusFactory;
    private final DependencyResolver resolver;

    public VirtualKafkaClusterReconciler(Clock clock, DependencyResolver resolver) {
        this.statusFactory = new VirtualKafkaClusterStatusFactory(clock);
        this.resolver = resolver;
    }

    @Override
    public UpdateControl<VirtualKafkaCluster> reconcile(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        ResolutionResult resolutionResult = resolver.resolveClusterRefs(cluster, context);
        ResolutionResult.ClusterResolutionResult clusterResolutionResult = resolutionResult.clusterResult(cluster)
                .orElseThrow(() -> new IllegalStateException("no resolution result for cluster " + name(cluster)));
        UpdateControl<VirtualKafkaCluster> updateControl;
        if (clusterResolutionResult.isFullyResolved()) {
            updateControl = maybeCombineStatusWithClusterConfigMap(cluster, context);
        }
        else {
            updateControl = createUnresolvedRefsStatus(cluster, clusterResolutionResult);
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(cluster), name(cluster));
        }
        return updateControl;
    }

    private @NonNull UpdateControl<VirtualKafkaCluster> maybeCombineStatusWithClusterConfigMap(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        UpdateControl<VirtualKafkaCluster> updateControl;
        updateControl = context
                .getSecondaryResource(ConfigMap.class)
                .flatMap(cm -> Optional.ofNullable(cm.getData()))
                .map(ProxyConfigStateData::new)
                .flatMap(data -> data.getStatusPatchForCluster(name(cluster)))
                .map(patch -> {
                    var rr = ResourceState.of(statusFactory.newTrueCondition(cluster, Condition.Type.ResolvedRefs));
                    var acc = ResourceState.fromList(patch.getStatus().getConditions());
                    return statusFactory.clusterStatusPatch(cluster, rr.replacementFor(acc));
                })
                .map(UpdateControl::patchStatus)
                .orElse(UpdateControl.noUpdate());
        return updateControl;
    }

    private @NonNull UpdateControl<VirtualKafkaCluster> createUnresolvedRefsStatus(VirtualKafkaCluster cluster,
                                                                                   ResolutionResult.ClusterResolutionResult clusterResolutionResult) {
        UpdateControl<VirtualKafkaCluster> updateControl;
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        UnresolvedReferences unresolvedReferences = clusterResolutionResult.unresolvedReferences();
        var unresovedIngressProxies = unresolvedReferences.getUnresolvedReferences(KAFKA_PROXY_INGRESS_KIND, KAFKA_PROXY_KIND).collect(Collectors.toSet());
        if (unresolvedReferences.anyDependenciesNotFoundFor(clusterRef)) {
            Stream<String> proxyMsg = refsMessage("spec.proxyRef references ", cluster,
                    unresolvedReferences.getUnresolvedReferences(clusterRef, KAFKA_PROXY_KIND));
            Stream<String> serviceMsg = refsMessage("spec.targetKafkaServiceRef references ", cluster,
                    unresolvedReferences.getUnresolvedReferences(clusterRef, KAFKA_SERVICE_KIND));
            Stream<String> ingressMsg = refsMessage("spec.ingressRefs references ", cluster,
                    unresolvedReferences.getUnresolvedReferences(clusterRef, KAFKA_PROXY_INGRESS_KIND));
            Stream<String> filterMsg = refsMessage("spec.filterRefs references ", cluster,
                    unresolvedReferences.getUnresolvedReferences(clusterRef, KAFKA_PROTOCOL_FILTER_KIND));
            updateControl = UpdateControl.patchStatus(statusFactory.newFalseConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, Condition.REASON_REFS_NOT_FOUND,
                    joiningMessages(proxyMsg, serviceMsg, ingressMsg, filterMsg)));
        }
        else if (unresolvedReferences.anyResolvedRefsConditionsFalse() || !unresovedIngressProxies.isEmpty()) {
            Stream<String> serviceMsg = refsMessage("spec.targetKafkaServiceRef references ", cluster,
                    unresolvedReferences.kafkaServicesWithResolvedRefsFalse().stream().map(ResourcesUtil::toLocalRef));
            Stream<String> ingressMsg = refsMessage("spec.ingressRefs references ", cluster,
                    unresolvedReferences.ingressesWithResolvedRefsFalse().stream().map(ResourcesUtil::toLocalRef));
            Stream<String> filterMsg = refsMessage("spec.filterRefs references ", cluster,
                    unresolvedReferences.filtersWithResolvedRefsFalse().stream().map(ResourcesUtil::toLocalRef));
            Stream<String> ingressProxyMessage = refsMessage("a spec.ingressRef had an inconsistent or missing proxyRef ", cluster,
                    unresolvedReferences.getUnresolvedReferences(KAFKA_PROXY_INGRESS_KIND, KAFKA_PROXY_KIND));
            updateControl = UpdateControl
                    .patchStatus(statusFactory.newFalseConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                            joiningMessages(serviceMsg, ingressMsg, filterMsg, ingressProxyMessage)));
        }
        else {
            updateControl = UpdateControl
                    .patchStatus(statusFactory.newFalseConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, Condition.REASON_INVALID,
                            joiningMessages(Stream.of("unknown dependency resolution issue"))));
        }
        return updateControl;
    }

    @NonNull
    private static String joiningMessages(
                                          Stream<String>... serviceMsg) {
        return Stream.of(serviceMsg).flatMap(Function.identity()).collect(Collectors.joining("; "));
    }

    @NonNull
    private static Stream<String> refsMessage(
                                              String prefix,
                                              VirtualKafkaCluster cluster,
                                              Stream<LocalRef<?>> refs) {
        List<LocalRef<?>> sortedRefs = refs.sorted().toList();
        return sortedRefs.isEmpty() ? Stream.of()
                : Stream.of(
                        prefix + sortedRefs.stream()
                                .map(ref -> ResourcesUtil.namespacedSlug(ref, cluster))
                                .collect(Collectors.joining(", ")));
    }

    @Override
    public List<EventSource<?, VirtualKafkaCluster>> prepareEventSources(EventSourceContext<VirtualKafkaCluster> context) {
        InformerEventSourceConfiguration<KafkaProxy> clusterToProxy = InformerEventSourceConfiguration.from(
                KafkaProxy.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(proxy -> ResourcesUtil.findReferrers(context,
                        proxy,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getProxyRef()))
                .build();

        InformerEventSourceConfiguration<ConfigMap> clusterToProxyConfigState = InformerEventSourceConfiguration.from(
                ConfigMap.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_CONFIG_STATE_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(VirtualKafkaClusterReconciler::toConfigStateResourceName)
                .withSecondaryToPrimaryMapper(configMap -> ResourcesUtil.findReferrers(context,
                        configMap,
                        VirtualKafkaCluster.class,
                        cluster -> new AnyLocalRefBuilder().withGroup("").withKind("ConfigMap")
                                .withName(cluster.getSpec().getProxyRef().getName() + CONFIG_STATE_CONFIG_MAP_SUFFIX)
                                .build()))
                .build();

        InformerEventSourceConfiguration<KafkaService> clusterToService = InformerEventSourceConfiguration.from(
                KafkaService.class,
                VirtualKafkaCluster.class)
                .withName(SERVICES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster,
                        cluster.getSpec().getTargetKafkaServiceRef()))
                .withSecondaryToPrimaryMapper(service -> ResourcesUtil.findReferrers(context,
                        service,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getTargetKafkaServiceRef()))
                .build();

        InformerEventSourceConfiguration<KafkaProxyIngress> clusterToIngresses = InformerEventSourceConfiguration.from(
                KafkaProxyIngress.class,
                VirtualKafkaCluster.class)
                .withName(INGRESSES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getIngressRefs)))
                .withSecondaryToPrimaryMapper(ingress -> ResourcesUtil.findReferrersMulti(context,
                        ingress,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getIngressRefs()))
                .build();

        InformerEventSourceConfiguration<KafkaProtocolFilter> clusterToFilters = InformerEventSourceConfiguration.from(
                KafkaProtocolFilter.class,
                VirtualKafkaCluster.class)
                .withName(FILTERS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getFilterRefs)))
                .withSecondaryToPrimaryMapper(filter -> ResourcesUtil.findReferrersMulti(context,
                        filter,
                        VirtualKafkaCluster.class,
                        cluster -> cluster.getSpec().getFilterRefs()))
                .build();

        return List.of(
                new InformerEventSource<>(clusterToProxy, context),
                new InformerEventSource<>(clusterToProxyConfigState, context),
                new InformerEventSource<>(clusterToIngresses, context),
                new InformerEventSource<>(clusterToService, context),
                new InformerEventSource<>(clusterToFilters, context));
    }

    @Override
    public ErrorStatusUpdateControl<VirtualKafkaCluster> updateErrorStatus(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<VirtualKafkaCluster> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(cluster), name(cluster), e.toString());
        }
        return uc;
    }

    private static Set<ResourceID> toConfigStateResourceName(VirtualKafkaCluster cluster) {
        return ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef())
                .stream().map(x -> new ResourceID(x.getName() + CONFIG_STATE_CONFIG_MAP_SUFFIX, x.getNamespace().orElse(null))).collect(Collectors.toSet());
    }

}
