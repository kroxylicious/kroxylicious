/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Escape-hatch {@link FilterSpec} for cases where a dedicated implementation is not justified.
 * Allows specifying a filter by its fully qualified class name and raw configuration object.
 * <p>
 * Fixture implementations handle this type generically — no per-filter knowledge required.
 *
 * @param filterClassName fully qualified filter class name
 * @param filterConfig    filter configuration object, or {@code null} if none
 */
public record RawFilterSpec(String filterClassName, @Nullable Object filterConfig) implements FilterSpec {

    public RawFilterSpec {
        Objects.requireNonNull(filterClassName, "filterClassName should not be null");
    }

    /**
     * Creates a {@code RawFilterSpec} with no configuration.
     *
     * @param filterClassName fully qualified filter class name
     */
    public RawFilterSpec(String filterClassName) {
        this(filterClassName, null);
    }
}