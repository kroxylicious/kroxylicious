/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedReference;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedReferences;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

/**
 * DependencyResolver resolves the dependencies of a KafkaProxy or VirtualKafkaCluster. We use numerous
 * Custom Resources to model the virtual clusters, ingresses, filters and clusters that will eventually
 * manifest as a single proxy Deployment.
 * <p>
 * DependencyResolver is responsible for resolving all of these references into Custom Resources, returning
 * a result that contains the resolved Custom Resource instances, and a description of which references could not
 * be resolved or were resolved but have a status condition declaring that the resource has unresolved
 * references.
 * </p>
 */
public class DependencyResolver {

    private static final ResolutionResult EMPTY_RESOLUTION_RESULT = new ResolutionResult(Map.of(), Map.of(), Map.of(), Set.of());

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
     * @return a resolution result containing all resolved resources, and a description of which resources could not be resolved
     */
    public ResolutionResult resolveProxyRefs(KafkaProxy proxy, Context<?> context) {
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(context);
        Set<VirtualKafkaCluster> virtualKafkaClusters = context.getSecondaryResources(VirtualKafkaCluster.class);
        return resolve(virtualKafkaClusters, Set.of(proxy), context);
    }

    /**
     * Resolves all dependencies of a VirtualKafkaCluster recursively (if there are dependencies
     * of dependencies, we resolve them too) and report if there were any unresolved dependencies.
     *
     * @param cluster cluster being resolved
     * @param context reconciliation context for a VirtualKafkaCluster
     * @return unresolved references
     */
    public UnresolvedReferences resolveClusterRefs(VirtualKafkaCluster cluster, Context<?> context) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(context);
        Set<KafkaProxy> proxies = context.getSecondaryResources(KafkaProxy.class);
        return resolve(Set.of(cluster), proxies, context).clusterResult(cluster)
                .orElseThrow(() -> new IllegalStateException("resolution result for cluster not found in result, should be impossible"))
                .unresolvedReferences();
    }

    private @NonNull ResolutionResult resolve(Set<VirtualKafkaCluster> virtualKafkaClusters, Set<KafkaProxy> proxies, Context<?> context) {
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }

        // TODO Replace this with the code from the VKCReconciler
        Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaService>, KafkaService> clusterRefs = context.getSecondaryResources(KafkaService.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters = context.getSecondaryResources(KafkaProtocolFilter.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        var resolutionResult = virtualKafkaClusters.stream()
                .map(cluster -> determineUnresolvedDependencies(cluster, ingresses, clusterRefs, filters, proxies))
                .collect(Collectors.toSet());
        return new ResolutionResult(filters, ingresses, clusterRefs, resolutionResult);
    }

    private ClusterResolutionResult determineUnresolvedDependencies(VirtualKafkaCluster cluster,
                                                                    Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                    Map<LocalRef<KafkaService>, KafkaService> services,
                                                                    Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters,
                                                                    Set<KafkaProxy> proxies) {
        VirtualKafkaClusterSpec spec = cluster.getSpec();
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        Set<UnresolvedReference> unresolved = new HashSet<>();
        determineUnresolvedIngresses(clusterRef, spec, ingresses).forEach(unresolved::add);
        determineIngressesWithUnresolvedProxies(ingresses, proxies).forEach(unresolved::add);
        determineUnresolvedKafkaProxies(clusterRef, spec, proxies).ifPresent(unresolved::add);
        determineUnresolvedKafkaService(clusterRef, spec, services).ifPresent(unresolved::add);
        determineUnresolvedFilters(clusterRef, spec, filters).forEach(unresolved::add);
        Set<KafkaProxyIngress> ingressesWithResolvedRefsFalse = ingresses.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of())))
                .collect(Collectors.toSet());
        Set<KafkaProtocolFilter> filtersWithResolvedRefsFalse = filters.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of())))
                .collect(Collectors.toSet());
        Set<KafkaService> kafkaServicesWithResolvedRefsFalse = services.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of())))
                .collect(Collectors.toSet());
        return new ClusterResolutionResult(cluster,
                new UnresolvedReferences(unresolved, ingressesWithResolvedRefsFalse, filtersWithResolvedRefsFalse, kafkaServicesWithResolvedRefsFalse));
    }

    private static boolean hasAnyResolvedRefsFalse(List<Condition> conditions) {
        return conditions.stream().anyMatch(Condition::isResolvedRefsFalse);
    }

    private Stream<UnresolvedReference> determineIngressesWithUnresolvedProxies(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                                Set<KafkaProxy> proxies) {
        Set<LocalRef<KafkaProxy>> localRefs = proxies.stream().map(ResourcesUtil::toLocalRef).collect(Collectors.toSet());
        return ingresses.values().stream().flatMap(ingress -> {
            ProxyRef proxyRef = ingress.getSpec().getProxyRef();
            if (!localRefs.contains(proxyRef)) {
                return Stream.of(new UnresolvedReference(toLocalRef(ingress), proxyRef));
            }
            else {
                return Stream.of();
            }
        });
    }

    private Optional<UnresolvedReference> determineUnresolvedKafkaProxies(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                          Set<KafkaProxy> proxies) {
        Set<LocalRef<KafkaProxy>> localRefs = proxies.stream().map(ResourcesUtil::toLocalRef).collect(Collectors.toSet());
        ProxyRef proxyRef = spec.getProxyRef();
        if (!localRefs.contains(proxyRef)) {
            return Optional.of(new UnresolvedReference(clusterRef, proxyRef));
        }
        else {
            return Optional.empty();
        }
    }

    private Stream<UnresolvedReference> determineUnresolvedFilters(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                   Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        List<FilterRef> filtersList = spec.getFilterRefs();
        if (filtersList == null) {
            return Stream.empty();
        }
        else {
            return filtersList.stream()
                    .filter(filterRef -> filters.values().stream().noneMatch(filterResource -> filterResourceMatchesRef(filterRef, filterResource)))
                    .map(ref -> new UnresolvedReference(clusterRef, ref));
        }
    }

    private Optional<UnresolvedReference> determineUnresolvedKafkaService(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                          Map<LocalRef<KafkaService>, KafkaService> clusterRefs) {
        var serviceRef = spec.getTargetKafkaServiceRef();
        if (!clusterRefs.containsKey(serviceRef)) {
            return Optional.of(new UnresolvedReference(clusterRef, serviceRef));
        }
        else {
            return Optional.empty();
        }
    }

    private static Stream<UnresolvedReference> determineUnresolvedIngresses(LocalRef<?> clusterRef, VirtualKafkaClusterSpec spec,
                                                                            Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses) {
        return spec.getIngressRefs().stream()
                .filter(ref -> !ingresses.containsKey(ref)).map(ingressRef -> new UnresolvedReference(clusterRef, ingressRef));
    }

    private static boolean filterResourceMatchesRef(FilterRef filterRef, KafkaProtocolFilter filterResource) {
        String apiVersion = filterResource.getApiVersion();
        var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
        return filterResourceGroup.equals(filterRef.getGroup())
                && filterResource.getKind().equals(filterRef.getKind())
                && name(filterResource).equals(filterRef.getName());
    }

}
