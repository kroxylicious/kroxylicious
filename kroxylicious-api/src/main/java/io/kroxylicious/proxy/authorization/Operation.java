/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An Operation is an {@code enum} of the possible operations on a resource of a particular type.
 * We use a one-enum-per-resource-type pattern so that the {@link Class} of an operation identifies the resource type.
 * @param <S>
 */
public interface Operation<S extends Enum<S> & Operation<S>> {

    default List<Action> actionsOf(Stream<String> names) {
        return names.map(topicName -> new Action(this, topicName)).toList();
    }

    default Set<S> implies() {
        return Set.of();
    }
}
