/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Set;

import io.kroxylicious.kubernetes.operator.ingress.IngressConflictException;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions.Status;
import static java.util.stream.Collectors.joining;

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

    public static ClusterCondition ingressConflict(String cluster, Set<IngressConflictException> ingressConflictExceptions) {
        String ingresses = ingressConflictExceptions.stream()
                .map(IngressConflictException::getIngressName)
                .collect(joining(","));
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, INVALID,
                String.format("Ingress(es) [%s] of cluster conflicts with another ingress", ingresses));
    }
}
