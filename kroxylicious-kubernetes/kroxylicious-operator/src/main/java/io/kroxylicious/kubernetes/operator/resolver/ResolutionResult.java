/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The result of attempting to resolve a reference
 *
 * @param <T> the referent type
 * @param referrer the referring resource represented as a LocalRef
 * @param reference the reference we attempted to resolve
 * @param referentNew the resolved referent resource, or null if the reference is dangling
 */
public record ResolutionResult<T extends HasMetadata>(LocalRef<?> referrer, LocalRef<T> reference, @Nullable T referentNew) {
    /**
     * If the resolution fails because we cannot locate a referent for a reference, we call this a dangling reference
     * @return true iff no referent was found for this reference
     */
    boolean dangling() {
        return referentNew == null;
    }

    /**
     * Returns the resolved referent resource, throwing if the reference is dangling.
     *
     * @return the resolved referent resource, never null
     */
    public T referentResource() {
        return maybeReferentResource().orElseThrow(() -> new NullPointerException("Referent resource for " + reference + " is null"));
    }

    /**
     * Returns the resolved referent resource as an {@link Optional}, empty if the reference is dangling.
     *
     * @return an optional containing the referent resource, or empty if unresolved
     */
    public Optional<T> maybeReferentResource() {
        return Optional.ofNullable(referentNew);
    }

    /**
     * Creates a successfully resolved result from the referring resource and the found referent.
     *
     * @param <T> the type of the referent resource
     * @param referrer the resource that holds the reference
     * @param referent the resource that was successfully resolved
     * @return a new resolution result representing a successful resolution
     */
    public static <T extends HasMetadata> ResolutionResult<T> resolved(HasMetadata referrer, T referent) {
        return new ResolutionResult<>(ResourcesUtil.toLocalRef(referrer), ResourcesUtil.toLocalRef(referent), referent);
    }
}
