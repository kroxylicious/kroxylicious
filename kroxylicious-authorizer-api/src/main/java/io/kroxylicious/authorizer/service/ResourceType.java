/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.Set;

/**
 * A {@code ResourceType} is an {@code enum} of the possible operations on a resource of a particular type.
 * We use a one-enum-per-resource-type pattern so that the {@link Class} an implementation also
 * serves to identify the resource type.
 * For this reason, implementations of this interface should be named for the type of resource
 * (for example {@code Topic}, or {@code ConsumerGroup}) rather than the operations
 * enumerated (so not {@code TopicOperations} or {@code ConsumerGroupOperations}).
 * @param <S> The self type.
 */
public interface ResourceType<S extends Enum<S> & ResourceType<S>> {

    /**
     * Returns a set of operations that are implied by this operation.
     * This must return the complete transitive closure of all such implied operations.
     * In other words, it's not allowed for
     * {@code OP_A.implies()} to contain {@code OP_B} and {@code OP_B.implies()} to contain {@code OP_C}
     * if {@code OP_A.implies()} does not contain {@code OP_C}.
     * Instead {@code OP_A.implies()} would have to contain both {@code OP_B} and {@code OP_C}.
     * @return The operations that are implied by this operation.
     */
    default Set<S> implies() {
        // TODO This is actually really tricky to model in a way that works for different Authorizer implementations
        // Allowing operations to express implication makes in-process authorization evaluations easier
        // because we can just call the method (either before or after querying internal data structures).
        // But it means an operation is more than just its name.
        // Which makes like harder for Authz-as-a-Service because either:
        // 1. they need to model the implication, in their backend representation of the rules
        // 2. Or else their Java client needs to use the implication to expand the set of actions being queried
        // prior to calling the service.
        // Either choice ends up coupling the Authz-as-a-Service Authorizer to particular Operation implementations
        return Set.of();
    }
}
