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
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult.DanglingReference;

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

    private static final ProxyResolutionResult EMPTY_RESOLUTION_RESULT = new ProxyResolutionResult(Map.of(), Map.of(), Map.of(), Set.of());

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
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }

        CommonDependencies commonDependencies = getCommonDependenciesFrom(context);
        var clusterResolutionResults = virtualKafkaClusters.stream()
                .map(cluster -> discoverProblemsAndBuildResolutionResult(cluster, commonDependencies, Set.of(proxy)))
                .map(DependencyResolver::checkClusterConditions)
                .collect(Collectors.toSet());
        return new ProxyResolutionResult(commonDependencies.filters(), commonDependencies.ingresses(), commonDependencies.kafkaServices(), clusterResolutionResults);
    }

    private static ClusterResolutionResult checkClusterConditions(ClusterResolutionResult result) {
        VirtualKafkaCluster cluster = result.cluster();
        boolean clusterHasResolvedRefsFalse = hasAnyResolvedRefsFalse(
                Optional.ofNullable(cluster.getStatus()).map(VirtualKafkaClusterStatus::getConditions).orElse(List.of()));
        Set<LocalRef<?>> resolvedRefsFalse = clusterHasResolvedRefsFalse ? Set.of(toLocalRef(cluster)) : Set.of();
        Set<LocalRef<?>> referentsWithStaleStatus = determineReferentsWithStaleStatus(cluster);
        return result.addAllResourcesHavingResolvedRefsFalse(resolvedRefsFalse).addReferentsWithStaleStatus(referentsWithStaleStatus);
    }

    private static CommonDependencies getCommonDependenciesFrom(Context<?> context) {
        Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaService>, KafkaService> clusterRefs = context.getSecondaryResources(KafkaService.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters = context.getSecondaryResources(KafkaProtocolFilter.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        return new CommonDependencies(ingresses, clusterRefs, filters);
    }

    // dependencies common to VirtualKafkaCluster and KafkaProxy reconciliation
    private record CommonDependencies(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                      Map<LocalRef<KafkaService>, KafkaService> kafkaServices,
                                      Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        private CommonDependencies {
            Objects.requireNonNull(ingresses);
            Objects.requireNonNull(kafkaServices);
            Objects.requireNonNull(filters);
        }
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
        CommonDependencies commonDependencies = getCommonDependenciesFrom(context);
        Set<KafkaProxy> proxies = context.getSecondaryResources(KafkaProxy.class);
        return discoverProblemsAndBuildResolutionResult(cluster, commonDependencies, proxies);
    }

    private ClusterResolutionResult discoverProblemsAndBuildResolutionResult(VirtualKafkaCluster cluster,
                                                                             CommonDependencies result,
                                                                             Set<KafkaProxy> proxies) {
        Set<DanglingReference> danglingReferences = determineDanglingRefs(result, proxies, cluster);
        Set<LocalRef<?>> resolvedRefsFalse = determineResolvedRefsFalse(result);
        Set<LocalRef<?>> referentsWithStaleStatus = determineReferentsWithStaleStatus(result);
        return new ClusterResolutionResult(cluster, danglingReferences, resolvedRefsFalse, referentsWithStaleStatus);
    }

    private static Set<LocalRef<?>> determineReferentsWithStaleStatus(CommonDependencies cluster) {
        Stream<LocalRef<?>> ingressesWithStaleStatus = cluster.ingresses().entrySet().stream().filter(e -> !ResourcesUtil.isStatusFresh(e.getValue()))
                .map(Map.Entry::getKey);
        Stream<LocalRef<?>> servicesWithStaleStatus = cluster.kafkaServices().entrySet().stream().filter(e -> !ResourcesUtil.isStatusFresh(e.getValue()))
                .map(Map.Entry::getKey);
        Stream<LocalRef<?>> filtersWithStaleStatus = cluster.filters().entrySet().stream().filter(e -> !ResourcesUtil.isStatusFresh(e.getValue()))
                .map(Map.Entry::getKey);
        return Stream.of(ingressesWithStaleStatus, servicesWithStaleStatus, filtersWithStaleStatus)
                .flatMap(Function.identity())
                .collect(Collectors.toSet());
    }

    private static Set<LocalRef<?>> determineReferentsWithStaleStatus(VirtualKafkaCluster cluster) {
        if (ResourcesUtil.isStatusFresh(cluster)) {
            return Set.of();
        }
        else {
            return Set.of(toLocalRef(cluster));
        }
    }

    private static Set<DanglingReference> determineDanglingRefs(CommonDependencies dependencies, Set<KafkaProxy> proxies,
                                                                VirtualKafkaCluster cluster) {
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        VirtualKafkaClusterSpec spec = cluster.getSpec();
        var danglingIngressRefs = determineDanglingIngressRefs(clusterRef, spec, dependencies.ingresses());
        var danglingProxyRefs = determineDanglingKafkaProxyRefs(clusterRef, spec, proxies).stream();
        var danglingIngressProxyRefs = determineIngressesWithDanglingProxyRefs(dependencies.ingresses(), proxies);
        var danglingServiceRefs = determineDanglingKafkaServiceRefs(clusterRef, spec, dependencies.kafkaServices()).stream();
        var danglingFilterRefs = determineDanglingFilterRefs(clusterRef, spec, dependencies.filters());
        return Stream.of(danglingIngressRefs, danglingProxyRefs, danglingIngressProxyRefs, danglingServiceRefs, danglingFilterRefs)
                .flatMap(Function.identity()).collect(Collectors.toSet());
    }

    private static Set<LocalRef<?>> determineResolvedRefsFalse(CommonDependencies dependencies) {
        Stream<HasMetadata> ingressesWithResolvedRefsFalse = dependencies.ingresses().values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of())))
                .map(HasMetadata.class::cast);
        Stream<HasMetadata> filtersWithResolvedRefsFalse = dependencies.filters().values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of())))
                .map(HasMetadata.class::cast);
        Stream<HasMetadata> kafkaServicesWithResolvedRefsFalse = dependencies.kafkaServices().values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of())))
                .map(HasMetadata.class::cast);
        Stream<LocalRef<?>> localRefStream = Stream
                .of(ingressesWithResolvedRefsFalse, filtersWithResolvedRefsFalse, kafkaServicesWithResolvedRefsFalse)
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
