/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.ProxyRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult.DanglingReference;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

/**
 * DependencyResolver resolves the dependencies of a KafkaProxy or VirtualKafkaCluster. We use numerous
 * Custom Resources to model the virtual clusters, ingresses, filters and clusters that will eventually
 * manifest as a single proxy Deployment.
 * <p>
 * DependencyResolver is responsible for resolving all of these references into Custom Resources, returning
 * a result that contains the resolved Custom Resource instances, and a description of any problems encountered.
 * Examples of problems are:
 * <ul>
 * <li>
 *     Dangling References - an entity refers to another entity that cannot be found
 * </li>
 * <li>
 *     ResolvedRefs=False condition - an entity has a condition declaring that it's Ref's are not resolved
 * </li>
 * </ul>
 */
public class DependencyResolver {

    private static final ProxyResolutionResult EMPTY_RESOLUTION_RESULT = new ProxyResolutionResult(Map.of(), Map.of(), Map.of(), Map.of());

    private DependencyResolver() {
    }

    public static DependencyResolver create() {
        return new DependencyResolver();
    }

    /**
     * Resolves all dependencies of a KafkaProxy recursively (if there are dependencies
     * of dependencies, we resolve them too). Makes the resolved dependencies available and
     * reports any problems during resolution.
     *
     * @param proxy proxy
     * @param context reconciliation context for a KafkaProxy
     * @return a resolution result containing all resolved resources, and a description of resolution problems, if any
     */
    public ProxyResolutionResult resolveProxyRefs(KafkaProxy proxy, Context<?> context) {
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(context);
        Set<VirtualKafkaCluster> virtualKafkaClusters = context.getSecondaryResources(VirtualKafkaCluster.class);
        return resolve(virtualKafkaClusters, Set.of(proxy), context);
    }

    /**
     * Resolves all dependencies of a VirtualKafkaCluster recursively (if there are dependencies
     * of dependencies, we resolve them too) and reports any problems during resolution.
     *
     * @param cluster cluster being resolved
     * @param context reconciliation context for a VirtualKafkaCluster
     * @return cluster resolution result containing a description of resolution problems, if any
     */
    public ClusterResolutionResult resolveClusterRefs(VirtualKafkaCluster cluster, Context<?> context) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(context);
        Set<KafkaProxy> proxies = context.getSecondaryResources(KafkaProxy.class);
        return resolve(Set.of(cluster), proxies, context).clusterResult(cluster)
                .orElseThrow(() -> new IllegalStateException("resolution result for cluster not found in result, should be impossible"));
    }

    private @NonNull ProxyResolutionResult resolve(Set<VirtualKafkaCluster> virtualKafkaClusters, Set<KafkaProxy> proxies, Context<?> context) {
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }

        Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaService>, KafkaService> clusterRefs = context.getSecondaryResources(KafkaService.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters = context.getSecondaryResources(KafkaProtocolFilter.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        var resolutionResult = virtualKafkaClusters.stream()
                .collect(Collectors.toMap(cluster -> cluster, cluster -> discoverProblemsAndBuildResolutionResult(cluster, ingresses, clusterRefs, filters, proxies)));
        return new ProxyResolutionResult(filters, ingresses, clusterRefs, resolutionResult);
    }

    private ClusterResolutionResult discoverProblemsAndBuildResolutionResult(VirtualKafkaCluster cluster,
                                                                             Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                             Map<LocalRef<KafkaService>, KafkaService> services,
                                                                             Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters,
                                                                             Set<KafkaProxy> proxies) {
        VirtualKafkaClusterSpec spec = cluster.getSpec();
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        Set<DanglingReference> danglingReferences = determineDanglingRefs(ingresses, services, filters, proxies, clusterRef, spec);
        return new ClusterResolutionResult(danglingReferences, determineResolvedRefsFalse(ingresses, services, filters));
    }

    private static @NonNull Set<DanglingReference> determineDanglingRefs(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                         Map<LocalRef<KafkaService>, KafkaService> services,
                                                                         Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters, Set<KafkaProxy> proxies,
                                                                         LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec) {
        var danglingIngressRefs = determineDanglingIngressRefs(clusterRef, spec, ingresses);
        var danglingProxyRefs = determineDanglingKafkaProxyRefs(clusterRef, spec, proxies).stream();
        var danglingIngressProxyRefs = determineIngressesWithDanglingProxyRefs(ingresses, proxies);
        var danglingServiceRefs = determineDanglingKafkaServiceRefs(clusterRef, spec, services).stream();
        var danglingFilterRefs = determineDanglingFilterRefs(clusterRef, spec, filters);
        return Stream.of(danglingIngressRefs, danglingProxyRefs, danglingIngressProxyRefs, danglingServiceRefs, danglingFilterRefs)
                .flatMap(Function.identity()).collect(Collectors.toSet());
    }

    private static @NonNull Set<LocalRef<?>> determineResolvedRefsFalse(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                        Map<LocalRef<KafkaService>, KafkaService> services,
                                                                        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        Stream<HasMetadata> ingressesWithResolvedRefsFalse = ingresses.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of())))
                .map(HasMetadata.class::cast);
        Stream<HasMetadata> filtersWithResolvedRefsFalse = filters.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of())))
                .map(HasMetadata.class::cast);
        Stream<HasMetadata> kafkaServicesWithResolvedRefsFalse = services.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of())))
                .map(HasMetadata.class::cast);
        Stream<LocalRef<?>> localRefStream = Stream.of(ingressesWithResolvedRefsFalse, filtersWithResolvedRefsFalse, kafkaServicesWithResolvedRefsFalse)
                .flatMap(Function.identity())
                .map(ResourcesUtil::toLocalRef);
        return localRefStream.collect(Collectors.toSet());
    }

    private static boolean hasAnyResolvedRefsFalse(List<Condition> conditions) {
        return conditions.stream().anyMatch(Condition::isResolvedRefsFalse);
    }

    private static Stream<DanglingReference> determineIngressesWithDanglingProxyRefs(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                                     Set<KafkaProxy> proxies) {
        Set<LocalRef<KafkaProxy>> localRefs = proxies.stream().map(ResourcesUtil::toLocalRef).collect(Collectors.toSet());
        return ingresses.values().stream().flatMap(ingress -> {
            ProxyRef proxyRef = ingress.getSpec().getProxyRef();
            if (!localRefs.contains(proxyRef)) {
                return Stream.of(new DanglingReference(toLocalRef(ingress), proxyRef));
            }
            else {
                return Stream.of();
            }
        });
    }

    private static Optional<DanglingReference> determineDanglingKafkaProxyRefs(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                               Set<KafkaProxy> proxies) {
        Set<LocalRef<KafkaProxy>> localRefs = proxies.stream().map(ResourcesUtil::toLocalRef).collect(Collectors.toSet());
        ProxyRef proxyRef = spec.getProxyRef();
        if (!localRefs.contains(proxyRef)) {
            return Optional.of(new DanglingReference(clusterRef, proxyRef));
        }
        else {
            return Optional.empty();
        }
    }

    private static Stream<DanglingReference> determineDanglingFilterRefs(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                         Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        List<FilterRef> filtersList = spec.getFilterRefs();
        if (filtersList == null) {
            return Stream.empty();
        }
        else {
            return filtersList.stream()
                    .filter(filterRef -> filters.values().stream().noneMatch(filterResource -> filterResourceMatchesRef(filterRef, filterResource)))
                    .map(ref -> new DanglingReference(clusterRef, ref));
        }
    }

    private static Optional<DanglingReference> determineDanglingKafkaServiceRefs(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                                 Map<LocalRef<KafkaService>, KafkaService> clusterRefs) {
        var serviceRef = spec.getTargetKafkaServiceRef();
        if (!clusterRefs.containsKey(serviceRef)) {
            return Optional.of(new DanglingReference(clusterRef, serviceRef));
        }
        else {
            return Optional.empty();
        }
    }

    private static Stream<DanglingReference> determineDanglingIngressRefs(LocalRef<?> clusterRef, VirtualKafkaClusterSpec spec,
                                                                          Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses) {
        return spec.getIngressRefs().stream()
                .filter(ref -> !ingresses.containsKey(ref)).map(ingressRef -> new DanglingReference(clusterRef, ingressRef));
    }

    private static boolean filterResourceMatchesRef(FilterRef filterRef, KafkaProtocolFilter filterResource) {
        String apiVersion = filterResource.getApiVersion();
        var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
        return filterResourceGroup.equals(filterRef.getGroup())
                && filterResource.getKind().equals(filterRef.getKind())
                && name(filterResource).equals(filterRef.getName());
    }

}
