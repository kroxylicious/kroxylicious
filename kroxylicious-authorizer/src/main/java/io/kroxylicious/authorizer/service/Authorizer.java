/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authentication.Subject;

/**
 * <p>Abstracts making an allow/deny decision about some {@link Subject} performing some {@link Action} on a resource.
 * In other words, this is an access control policy decision point.</p>
 *
 * <p>{@code Authorizer} is a flexible abstraction, while it assumes that resources have names, it
 * doesn't prescribe any specific kinds of resource or operations. Instead, resource kinds and the operations they support
 * are represented as subclasses of {@link ResourceType}.</p>
 */
public interface Authorizer {

    /**
     * Determines whether the given {@code subject} is allowed to perform the given {@code actions}.
     * The implementation must ensure that the returned authorization partitions all the given {@code actions}
     * between {@link AuthorizeResult#allowed()} and {@link AuthorizeResult#denied()}.
     * @param subject The subject.
     * @param actions The actions.
     * @return The outcome.
     */
    CompletionStage<AuthorizeResult> authorize(Subject subject, List<Action> actions);

    /**
     * <p>Returns the types of resource that this authorizer is able to make decisions about.
     * If this is not known to the implementation it should return empty.</p>
     *
     * <p>This is provided so that an access control policy enforcement point can confirm that it
     * is capable of providing access control to all the resource types in the access control policy
     * backing this authorizer.</p>
     *
     * @return the types of resource that this authorizer is able to make decisions about.
     */
    Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes();

}
