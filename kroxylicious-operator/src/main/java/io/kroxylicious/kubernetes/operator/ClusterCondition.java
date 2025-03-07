/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Locale;
import java.util.Optional;

import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.TargetCluster;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions.Status;

public record ClusterCondition(@NonNull String cluster,
                               @NonNull ConditionType type,
                               @NonNull Status status,
                               @Nullable String reason,
                               @Nullable String message) {

    public static final String INVALID = "Invalid";

    static ClusterCondition accepted(String cluster) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.TRUE, null, null);
    }

    static ClusterCondition filterInvalid(String cluster, String filterName, String detail) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" is invalid: %s", filterName, detail));
    }

    static ClusterCondition filterNotFound(String cluster, String filterName) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" does not exist.", filterName));
    }

    static ClusterCondition filterKindNotKnown(String cluster, String filterName, String kind) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" has a kind %s that is not known to this operator.", filterName, kind));
    }

    static ClusterCondition targetClusterRefNotFound(String cluster, TargetCluster targetCluster) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Target Cluster \"%s\" does not exist.", kubeName(targetCluster).orElse("<unknown>")));
    }

    @NonNull
    private static Optional<String> kubeName(TargetCluster targetCluster) {
        return Optional.ofNullable(targetCluster.getClusterRef())
                .map(r -> {
                    var group = Optional.ofNullable(r.getGroup()).map(".%s"::formatted).orElse("");
                    return "%s%s/%s".formatted(r.getKind().toLowerCase(Locale.ROOT), group, r.getName());
                });
    }

}
