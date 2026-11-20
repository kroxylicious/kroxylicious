/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import java.util.Objects;

import io.kroxylicious.systemtests.api.FilterSpec;

/**
 * {@link FilterSpec} for the simple-transform filter used in operator and smoke tests.
 * Configures a find-and-replace transformation on produce request record values.
 *
 * @param findPattern      regex pattern to match in record values
 * @param replacementValue replacement string
 */
public record SimpleTransformFilterSpec(String findPattern, String replacementValue) implements FilterSpec {

    public SimpleTransformFilterSpec {
        Objects.requireNonNull(findPattern, "findPattern must not be null");
        Objects.requireNonNull(replacementValue, "replacementValue must not be null");
    }
}