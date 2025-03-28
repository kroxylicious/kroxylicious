/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Arrays;
import java.util.Set;

import io.kroxylicious.kubernetes.api.common.AnyLocalRef;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.operator.model.ingress.IngressConflictException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

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

    static ClusterCondition filterInvalid(String cluster, String filterName, String detail) {
        return new ClusterCondition(cluster, Condition.Type.Accepted, Condition.Status.FALSE, INVALID,
                String.format("Filter \"%s\" is invalid: %s", filterName, detail));
    }

    public static ClusterCondition refNotFound(String cluster, LocalRef<?> ref) {
        return new ClusterCondition(cluster, Condition.Type.ResolvedRefs, Condition.Status.FALSE, INVALID,
                String.format("Resource of kind \"%s\" in group \"%s\" named \"%s\" does not exist.", ref.getKind(), ref.getGroup(), ref.getName()));
    }

    static ClusterCondition refNotSupported(String cluster,
                                            AnyLocalRef ref,
                                            String path,
                                            String... supported) {
        return new ClusterCondition(cluster, Condition.Type.Accepted, Condition.Status.FALSE, INVALID,
                String.format("Resource of kind \"%s\" in group \"%s\" named \"%s\" referenced from  %s is not supported. "
                        + "Supported kinds and groups: %s",
                        ref.getKind(), ref.getGroup(), ref.getName(), path,
                        Arrays.stream(supported).collect(joining(", "))));
    }

    public static ClusterCondition ingressConflict(String cluster, Set<IngressConflictException> ingressConflictExceptions) {
        String ingresses = ingressConflictExceptions.stream()
                .map(IngressConflictException::getIngressName)
                .collect(joining(","));
        return new ClusterCondition(cluster, Condition.Type.Accepted, Condition.Status.FALSE, INVALID,
                String.format("Ingress(es) [%s] of cluster conflicts with another ingress", ingresses));
    }

}
