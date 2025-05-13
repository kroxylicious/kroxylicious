/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static java.util.Comparator.comparing;

/**
 * The result of a deep resolution of the transitive references of a single KafkaProxy. Contains all
 * Filters, KafkaProxyIngresses and KafkaServices that were successfully resolved. It also
 * describes which referents could not be resolved per VirtualKafkaCluster.
 */
public class ProxyResolutionResult {
    private final Set<ClusterResolutionResult> clusterResolutionResults;

    ProxyResolutionResult(Set<ClusterResolutionResult> clusterResolutionResults) {
        Objects.requireNonNull(clusterResolutionResults);
        this.clusterResolutionResults = clusterResolutionResults;
    }

    /**
     * Get all VirtualKafkaClusters that could have all their transitive references resolved, sorted by the VirtualKafkaCluster's metadata.name
     * @return non-null list of VirtualKafkaClusters sorted by metadata.name
     */
    public List<VirtualKafkaCluster> fullyResolvedClustersInNameOrder() {
        return clustersSatisfying(
                clusterResolutionResult -> clusterResolutionResult.allReferentsFullyResolved() && !ResourcesUtil.hasResolvedRefsFalseCondition(
                        clusterResolutionResult.cluster()));
    }

    /**
     * Get all VirtualKafkaClusters, even if they have dangling references, sorted by the VirtualKafkaCluster's metadata.name
     * @return non-null list of VirtualKafkaClusters sorted by metadata.name
     */
    public List<VirtualKafkaCluster> allClustersInNameOrder() {
        return clustersSatisfying(result -> true);
    }

    private List<VirtualKafkaCluster> clustersSatisfying(Predicate<ClusterResolutionResult> include) {
        return clusterResolutionResults.stream().filter(include)
                .sorted(comparing(r -> ResourcesUtil.name(r.cluster()))).map(ClusterResolutionResult::cluster).toList();
    }

    /**
     * Get all ClusterResolutionResult
     * @return all ClusterResolutionResult
     */
    public Set<ClusterResolutionResult> clusterResolutionResults() {
        return clusterResolutionResults;
    }

    public boolean allReferentsHaveFreshStatus() {
        return clusterResolutionResults.stream().allMatch(
                clusterResolutionResult -> clusterResolutionResult.allResolvedReferents()
                        .allMatch(ResourcesUtil::isStatusFresh)
                        && ResourcesUtil.isStatusFresh(
                                clusterResolutionResult.cluster()));
    }

    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> allReferentsWithStaleStatus() {
        return clusterResolutionResults.stream().flatMap(clusterResolutionResult -> {
            Stream<LocalRef<?>> referentsWithStaleStatus = clusterResolutionResult.allResolvedReferents()
                    .filter(ResourcesUtil.isStatusStale())
                    .map(ResourcesUtil::toLocalRef);
            Stream<LocalRef<?>> clusterWithStaleStatus = !ResourcesUtil.isStatusFresh(clusterResolutionResult.cluster())
                    ? Stream.of(ResourcesUtil.toLocalRef(clusterResolutionResult.cluster()))
                    : Stream.of();
            return Stream.concat(referentsWithStaleStatus, clusterWithStaleStatus);
        });
    }

    public Stream<ClusterResolutionResult> allResolutionResultsInClusterNameOrder() {
        return clusterResolutionResults.stream().sorted(comparing(r -> ResourcesUtil.name(r.cluster())));
    }
}
