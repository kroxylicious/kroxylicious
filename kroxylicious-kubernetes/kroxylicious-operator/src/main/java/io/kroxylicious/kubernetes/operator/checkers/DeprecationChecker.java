/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Checks a custom resource for deprecations.
 * <p>
 * If found, uses {@link DeprecationCheckContext#addCondition} to add a
 * status condition to the custom resource.
 *
 * @param <S> the spec type of the custom resource
 * @param <T> the status type of the custom resource
 * @param <R> the custom resource type
 * @param <F> the status factory type used to construct conditions
 *
 * @see DeprecationCheckContext
 */
public interface DeprecationChecker<S, T, R extends CustomResource<S, T>, F extends StatusFactory<R>> {

    /**
     * Performs the deprecation check against {@link DeprecationCheckContext#resource()}.
     * <p>
     * Implementations should call {@link DeprecationCheckContext#addCondition} for each
     * deprecation detected.
     *
     * @param context see {@link DeprecationCheckContext}
     */
    void check(DeprecationCheckContext<S, T, R, F> context);

}