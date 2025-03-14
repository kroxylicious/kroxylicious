/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.TargetCluster;
import io.kroxylicious.kubernetes.operator.model.ingress.IngressConflictException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions.Status;
import static java.util.stream.Collectors.joining;

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

    public static ClusterCondition filterNotFound(String cluster, String filterName) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Filter \"%s\" does not exist.", filterName));
    }

    public static ClusterCondition ingressNotFound(String cluster, String ingressName) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("KafkaProxyIngress \"%s\" does not exist.", ingressName));
    }

    public static ClusterCondition ingressConflict(String cluster, Set<IngressConflictException> ingressConflictExceptions) {
        String ingresses = ingressConflictExceptions.stream()
                .map(IngressConflictException::getIngressName)
                .collect(joining(","));
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Ingress(es) [%s] of cluster conflicts with another ingress", ingresses));
    }

    public static ClusterCondition targetClusterRefNotFound(String cluster, TargetCluster targetCluster) {
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
