/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

/**
 * The result of a {@link FieldPopulationStrategy#resolve} call.
 */
public sealed interface FieldDecision permits FieldDecision.Defer, FieldDecision.Value {

    /**
     * The strategy has no opinion on this field; the walker should apply its default handling
     * (for a scalar leaf, defer to the next strategy in the chain; for a composite/array field, recurse).
     */
    record Defer() implements FieldDecision {}

    /**
     * Set the field to {@code value}, which may itself be {@code null}.
     *
     * @param value the value to set
     */
    record Value(Object value) implements FieldDecision {}
}
