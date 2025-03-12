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
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Filters;
import io.kroxylicious.kubernetes.operator.ClusterCondition;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SharedKafkaProxyContext;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedDependency;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ClusterCondition.ingressNotFound;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.FILTER;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.KAFKA_CLUSTER_REF;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.KAFKA_PROXY_INGRESS;

public class DependencyResolver {

    public static final ResolutionResult EMPTY_RESOLUTION_RESULT = new ResolutionResult(Map.of(), Map.of(), Map.of(), Map.of());

    public static ResolutionResult deepResolve(@NonNull Context<KafkaProxy> context) {
        Objects.requireNonNull(context);
        Set<VirtualKafkaCluster> virtualKafkaClusters = context.getSecondaryResources(VirtualKafkaCluster.class);
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }
        Map<String, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream().collect(toByNameMap());
        Map<String, KafkaClusterRef> clusterRefs = context.getSecondaryResources(KafkaClusterRef.class).stream().collect(toByNameMap());
        Map<String, GenericKubernetesResource> filters = context.getSecondaryResources(GenericKubernetesResource.class).stream().collect(toByNameMap());
        var resolutionResult = virtualKafkaClusters.stream().map(cluster -> {
            VirtualKafkaClusterSpec spec = cluster.getSpec();
            Set<UnresolvedDependency> unresolvedDependencies = new HashSet<>();
            spec.getIngressRefs().stream()
                    .filter(ref -> !ingresses.containsKey(ref.getName()))
                    .map(ref -> new UnresolvedDependency(KAFKA_PROXY_INGRESS, ref.getName())).forEach(unresolvedDependencies::add);
            String clusterRef = spec.getTargetCluster().getClusterRef().getName();
            if (!clusterRefs.containsKey(clusterRef)) {
                unresolvedDependencies.add(new UnresolvedDependency(KAFKA_CLUSTER_REF, clusterRef));
            }
            List<Filters> filtersList = spec.getFilters();
            if (filtersList != null) {
                filtersList.stream()
                        .filter(filterRef -> filters.values().stream().noneMatch(filterResource -> filterResourceMatchesRef(filterRef, filterResource)))
                        .map(ref -> new UnresolvedDependency(FILTER, ref.getName())).forEach(unresolvedDependencies::add);
            }
            return new ResolutionResult.ClusterResolutionResult(cluster, unresolvedDependencies);
        }).collect(Collectors.toMap(result -> ResourcesUtil.name(result.cluster()), r -> r));
        ResolutionResult result = new ResolutionResult(filters, ingresses, clusterRefs, resolutionResult);
        reportClustersThatDidNotFullyResolve(context, result);
        return result;
    }

    private static boolean filterResourceMatchesRef(Filters filterRef, GenericKubernetesResource filterResource) {
        String apiVersion = filterResource.getApiVersion();
        var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
        return filterResourceGroup.equals(filterRef.getGroup())
                && filterResource.getKind().equals(filterRef.getKind())
                && name(filterResource).equals(filterRef.getName());
    }

    public static void reportClustersThatDidNotFullyResolve(Context<KafkaProxy> context, ResolutionResult resolutionResult) {
        resolutionResult.clusterResult()
                .filter(ResolutionResult.ClusterResolutionResult::isAnyDependencyUnresolved)
                .forEach(clusterResolutionResult -> {
                    clusterResolutionResult.firstUnresolvedDependency().ifPresent(unresolved -> {
                        VirtualKafkaCluster cluster = clusterResolutionResult.cluster();
                        switch (unresolved.type()) {
                            case KAFKA_PROXY_INGRESS -> {
                                SharedKafkaProxyContext.addClusterCondition(context, cluster, ingressNotFound(name(cluster), unresolved.name()));
                            }
                            case FILTER -> {
                                SharedKafkaProxyContext.addClusterCondition(context, cluster, ClusterCondition.filterNotFound(name(cluster), unresolved.name()));
                            }
                            case KAFKA_CLUSTER_REF -> {
                                SharedKafkaProxyContext.addClusterCondition(context, cluster,
                                        ClusterCondition.targetClusterRefNotFound(name(cluster), cluster.getSpec().getTargetCluster()));
                            }
                        }
                    });
                });
    }
}
