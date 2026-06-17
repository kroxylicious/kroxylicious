/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator;

import java.util.function.UnaryOperator;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Drives the status state that a reconciler external to the one under test would produce.
 * <p>
 * "External" here means external to the reconciler being tested — this includes both
 * truly external controllers (e.g. Strimzi setting status on a {@code Kafka} resource)
 * and sibling Kroxylicious reconcilers whose output the reconciler-under-test depends on.
 * Using this class in tests ensures that dependent state is exactly as required, without
 * relying on other reconcilers to run.
 * <p>
 * Obtain instances via {@link LocalKroxyliciousOperatorExtension#externalOperator()}.
 */
public class ExternalOperator {

    private final KubernetesClient client;
    private final String namespace;

    ExternalOperator(KubernetesClient client, String namespace) {
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
     * @throws IllegalStateException if the named resource does not exist
     */
    public <T extends HasMetadata> T updateStatus(Class<T> type, String name, UnaryOperator<T> statusMutator) {
        T fresh = client.resources(type).inNamespace(namespace).withName(name).get();
        if (fresh == null) {
            throw new IllegalStateException("expected %s '%s' to exist".formatted(type.getSimpleName(), name));
        }
        T mutated = statusMutator.apply(fresh);
        return client.resource(mutated).inNamespace(namespace).patchStatus();
    }
}
