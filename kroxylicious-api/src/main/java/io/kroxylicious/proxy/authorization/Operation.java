/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.util.List;

/**
 * An Operation is an {@code enum} of the possible operations on a resource of a particular type.
 * We use a one-enum-per-resource-type pattern so that the {@link Class} of an operation identifies the resource type.
 * @param <S>
 */
public interface Operation<S extends Enum<S> & Operation<S>> {

    default List<Action> of(List<String> topicNames) {
        return topicNames.stream().map(topicName -> new Action(this, topicName)).toList();
    }
}
