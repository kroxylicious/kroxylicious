/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

/**
 * The outcome of a {@link MessagePopulator} run.
 */
public sealed interface PopulationResult permits PopulationResult.Populated, PopulationResult.Failed {

    /**
     * Every field the populator visited was successfully set.
     */
    record Populated() implements PopulationResult {}

    /**
     * Setting a field failed. The populator makes no judgement about whether this is expected;
     * that's for the caller to decide.
     *
     * @param fieldPath the field that failed
     * @param attemptedValue the value that was attempted
     * @param cause the failure
     */
    record Failed(FieldPath fieldPath, Object attemptedValue, Throwable cause) implements PopulationResult {}
}
