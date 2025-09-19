/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.util.List;

public interface Authorizer {

    /**
     * Determines whether the given {@code subject} is allowed to perform the given {@code actions}.
     * The implementation must ensure that the returned authorization partitions all the given actions
     * between {@link Authorization#allowed()} and {@link Authorization#denied()}.
     * @param subject The subject.
     * @param actions The actions.
     * @return The outcome.
     */
    Authorization authorize(Subject subject, List<Action> actions);

    static void main(String[] args) {
        // Config schema and parser
        // resource name patterns

        // At start up (or later) we want to validate the permissions given
//        for (ResourceType type : ServiceLoader.load(ResourceType.class)) {
//            // TODO validate that all the names are unique
//            type.resourceTypeName();
//            // TODO validate that all the operations are supported
//            if (!(Enum.valueOf(type.operationType(), "READ") instanceof Operation<?>)) {
//                throw new RuntimeException();
//            }
//            // Is this only about the ResourceType, or is it actually a property of the authorizer
//            // which resource types it supports
//        }

        // A plugin might want to depend on a particular resource type that it doesn't define. How does it do that?




    }
}
