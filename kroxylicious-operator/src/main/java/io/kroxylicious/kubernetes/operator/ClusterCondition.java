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

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions.Status;

public record ClusterCondition(String cluster, ConditionType type, Status status, String reason, String message) {

    public static final String INVALID = "Invalid";

    static ClusterCondition accepted(String cluster) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.TRUE, null, null);
    }

    static ClusterCondition filterInvalid(String cluster, String filterName, String reason) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" is invalid: %s", filterName, reason));
    }

    static ClusterCondition filterNotExists(String cluster, String filterName) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" does not exist.", filterName));
    }

    static ClusterCondition filterKindNotKnown(String cluster, String filterName, String kind) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" has a kind %s that is not known to this operator.", filterName, kind));
    }

    static ClusterCondition targetClusterRefNotExists(String cluster, TargetCluster targetCluster) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Target Cluster \"%s\" does not exist.", kubeName(targetCluster).orElse("<unknown>")));
    }

    @NonNull
    private static Optional<String> kubeName(TargetCluster targetCluster) {
        return Optional.ofNullable(targetCluster.getClusterRef())
                .map(r -> "%s%s/%s".formatted(r.getKind().toLowerCase(Locale.ROOT), Optional.ofNullable(r.getGroup()).map(
                        ".%s"::formatted).orElse(""), r.getName()));
    }

}
