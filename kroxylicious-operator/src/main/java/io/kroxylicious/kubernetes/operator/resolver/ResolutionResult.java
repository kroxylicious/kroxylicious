/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.LocalRef;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The result of attempting to resolve a reference
 *
 * @param <T> the referent type
 * @param referrer the referring resource represented as a LocalRef
 * @param reference the reference we attempted to resolve
 */
public record ResolutionResult<T extends HasMetadata>(LocalRef<?> referrer, LocalRef<T> reference, @Nullable T referentNew) {
    /**
     * If the resolution fails because we cannot locate a referent for a reference, we call this a dangling reference
     * @return true iff no referent was found for this reference
     */
    boolean dangling() {
        return referentNew == null;
    }

    public T referentResource() {
        return maybeReferentResource().orElseThrow(() -> new NullPointerException("Referent resource for " + reference + " is null"));
    }

    public Optional<T> maybeReferentResource() {
        return Optional.ofNullable(referentNew);
    }
}
