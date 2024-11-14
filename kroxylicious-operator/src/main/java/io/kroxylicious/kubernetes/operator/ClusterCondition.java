/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions.Status;

public record ClusterCondition(String cluster, ConditionType type, Status status, String reason, String message) {

    static ClusterCondition accepted(String cluster) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.TRUE, null, null);
    }

    static ClusterCondition filterNotExists(String cluster, String filterName) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, "Invalid",
                "Filter \"" + filterName + "\" does not exist.");
    }

    static ClusterCondition filterKindNotKnown(String cluster, String filterName, String kind) {
        return new ClusterCondition(cluster, ConditionType.Accepted, Status.FALSE, "Invalid",
                "Filter \"" + filterName + "\" has a kind \"" + kind + "\" that is not known to this operator.");
    }

}
