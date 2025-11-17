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

    Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes();

}
