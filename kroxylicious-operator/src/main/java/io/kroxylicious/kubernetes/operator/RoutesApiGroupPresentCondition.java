/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.concurrent.atomic.AtomicReference;

import io.fabric8.kubernetes.api.model.APIGroup;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;

import io.kroxylicious.proxy.tag.VisibleForTesting;

public class RoutesApiGroupPresentCondition<R extends HasMetadata, P extends HasMetadata> implements Condition<R, P> {

    private enum State {
        INITIAL,
        MISSING,
        PRESENT
    }

    private static final AtomicReference<State> isPresentCache = new AtomicReference<>(State.INITIAL);

    RoutesApiGroupPresentCondition() {
    }

    public boolean isMet(DependentResource<R, P> dependentResource, P primary, Context<P> context) {
        State state = isPresentCache.get();
        if (state == State.MISSING) {
            return false;
        }
        else if (state == State.PRESENT) {
            return true;
        }
        else {
            State newState = discoverIfPresent("route.openshift.io", "v1", context.getClient());
            isPresentCache.compareAndExchange(State.INITIAL, newState);
            return isPresentCache.get() == State.PRESENT;
        }
    }

    private static State discoverIfPresent(String group, String version, KubernetesClient client) {
        APIGroup apiGroup = client.getApiGroup(group);
        if (apiGroup != null) {
            if (apiGroup.getVersions().stream().anyMatch(v -> version.equals(v.getVersion()))) {
                return State.PRESENT;
            }
        }
        return State.MISSING;
    }

    @VisibleForTesting
    static void reset() {
        isPresentCache.set(State.INITIAL);
    }
}
