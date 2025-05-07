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
 * The result of attempting to dereference a reference
 * @param referrer the referring resource's LocalRef
 * @param reference the reference we attempted to dereference
 * @param referent the referent if the attempt to dereference was successful, null otherwise
 * @param <T> the referent type
 */
public record ResolutionResult<T extends HasMetadata>(LocalRef<?> referrer, LocalRef<T> reference, @Nullable Referent<T> referent) {
    /**
     * If the dereference fails because we cannot locate a referent for a reference, we call this a dangling reference
     * @return true iff no referent was found for this reference
     */
    boolean dangling() {
        return referent == null;
    }

    public T referentResource() {
        if (referent == null) {
            throw new IllegalStateException("Referent is null!");
        }
        return referent.resource();
    }

    public Optional<T> maybeReferentResource() {
        return Optional.ofNullable(referent).map(Referent::resource);
    }
}
