/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Set;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.operator.model.ingress.IngressConflictException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static java.util.stream.Collectors.joining;

public record ClusterCondition(@NonNull String cluster,
                               @NonNull Condition.Type type,
                               @NonNull Condition.Status status,
                               @Nullable String reason,
                               @Nullable String message) {

    public static final String INVALID = "Invalid";

    static ClusterCondition accepted(String cluster) {
        return new ClusterCondition(cluster, Condition.Type.Accepted, Condition.Status.TRUE, null, null);
    }

    public static ClusterCondition refNotFound(HasMetadata referrer, LocalRef<?> ref) {
        return new ClusterCondition(name(referrer), Condition.Type.ResolvedRefs, Condition.Status.FALSE, INVALID,
                String.format("Resource %s was not found.", ResourcesUtil.namespacedSlug(ref, referrer)));
    }

    public static ClusterCondition ingressConflict(String cluster, Set<IngressConflictException> ingressConflictExceptions) {
        String ingresses = ingressConflictExceptions.stream()
                .map(IngressConflictException::getIngressName)
                .collect(joining(","));
        return new ClusterCondition(cluster, Condition.Type.Accepted, Condition.Status.FALSE, INVALID,
                String.format("Ingress(es) [%s] of cluster conflicts with another ingress", ingresses));
    }

}
