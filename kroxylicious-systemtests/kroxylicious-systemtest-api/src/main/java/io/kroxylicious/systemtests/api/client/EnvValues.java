/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Formats the accepted values of an environment variable for error messages.
 */
final class EnvValues {

    private EnvValues() {
    }

    static <E> String of(E[] values, Function<E, String> envValue) {
        return Arrays.stream(values).map(envValue).collect(Collectors.joining(", ", "[", "]"));
    }
}
