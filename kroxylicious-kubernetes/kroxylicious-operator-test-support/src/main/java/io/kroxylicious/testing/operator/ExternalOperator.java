/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator;

import java.util.Objects;
import java.util.function.UnaryOperator;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Simulates an external Kubernetes controller that updates status subresources.
 * <p>
 * In production, some resources (e.g. Strimzi {@code Kafka}) have their status set by their
 * own operator. In integration tests, this class stands in for those external controllers,
 * providing the status preconditions that the operator-under-test observes.
 * <p>
 * Obtain instances via {@link LocalKroxyliciousOperatorExtension#externalOperator()}.
 */
public class ExternalOperator {

    private final KubernetesClient client;
    private final String namespace;

    ExternalOperator(@NonNull KubernetesClient client, @NonNull String namespace) {
        this.client = client;
        this.namespace = namespace;
    }

    /**
     * Re-fetches the resource by name, applies the given mutator to set status, then
     * patches the status subresource. Re-fetching avoids 409 Conflict errors when
     * the resourceVersion has advanced since any earlier fetch.
     *
     * @param type          resource class
     * @param name          resource name
     * @param statusMutator sets status on the fresh resource and returns it
     * @return the resource after status patch
     * @throws NullPointerException if the named resource does not exist
     */
    @NonNull
    public <T extends HasMetadata> T updateStatus(@NonNull Class<T> type, @NonNull String name, @NonNull UnaryOperator<T> statusMutator) {
        T fresh = Objects.requireNonNull(
                client.resources(type).inNamespace(namespace).withName(name).get(),
                () -> "expected %s '%s' to exist".formatted(type.getSimpleName(), name));
        T mutated = statusMutator.apply(fresh);
        return client.resource(mutated).inNamespace(namespace).patchStatus();
    }
}