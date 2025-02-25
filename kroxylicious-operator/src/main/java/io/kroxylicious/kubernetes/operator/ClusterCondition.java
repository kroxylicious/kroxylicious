/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.clusters.Conditions.Status;

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

}
