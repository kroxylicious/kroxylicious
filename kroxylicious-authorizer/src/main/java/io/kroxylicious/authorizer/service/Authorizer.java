/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.authentication.Subject;

public interface Authorizer {

    /**
     * Determines which operations the given {@code subject} is allowed to perform on the given {@code resources}
     * @param subject The subject.
     * @param resources A map of operation class to list of resource names.
     * @return The authorization.
     */
    default CompletionStage<Authorization> authorizedOperations(Subject subject, Map<Class<? extends Operation<?>>, List<String>> resources) {
        var actions = resources.entrySet().stream().flatMap(entry -> Arrays.stream(entry.getKey().getEnumConstants())
                .flatMap(enumConstant -> entry.getValue().stream().map(resourceName -> new Action(enumConstant, resourceName)))).distinct().toList();
        return authorize(subject, actions);
    }

    /**
     * Determines whether the given {@code subject} is allowed to perform the given {@code actions}.
     * The implementation must ensure that the returned authorization partitions all the given {@code actions}
     * between {@link Authorization#allowed()} and {@link Authorization#denied()}.
     * @param subject The subject.
     * @param actions The actions.
     * @return The outcome.
     */
    CompletionStage<Authorization> authorize(Subject subject, List<Action> actions);

}
