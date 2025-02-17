/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class TopicNameKekSelectionTest {

    public static Stream<Arguments> invalidConstructorArgs() {
        Arguments nullSelection = Arguments.of(null, Set.of(), NullPointerException.class);
        Arguments nullUnresolved = Arguments.of(Map.of(), null, NullPointerException.class);
        HashMap<Object, Object> map = new HashMap<>();
        map.put("abc", null);
        Arguments nullMapValue = Arguments.of(map, Set.of(), IllegalArgumentException.class);
        return Stream.of(nullSelection, nullUnresolved, nullMapValue);
    }

    @ParameterizedTest
    @MethodSource
    void invalidConstructorArgs(Map<String, Integer> selection, Set<String> unresolvedTopicNames, Class<Exception> exceptionClass) {
        Assertions.assertThatThrownBy(() -> new TopicNameKekSelection<>(selection, unresolvedTopicNames)).isInstanceOf(exceptionClass);
    }

    @Test
    void validConstructor() {
        Map<String, Integer> selection = Map.of("abc", 1);
        Set<String> unresolved = Set.of("def");
        TopicNameKekSelection<Integer> kekSelection = new TopicNameKekSelection<>(selection, unresolved);
        assertThat(kekSelection.topicNameToKekId()).isEqualTo(selection);
        assertThat(kekSelection.unresolvedTopicNames()).isEqualTo(unresolved);
    }
}