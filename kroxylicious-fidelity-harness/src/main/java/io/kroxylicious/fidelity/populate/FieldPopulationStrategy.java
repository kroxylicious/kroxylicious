/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import org.apache.kafka.common.protocol.types.BoundField;

/**
 * Decides what value, if any, to populate a schema field with.
 */
public interface FieldPopulationStrategy {

    /**
     * Resolves how {@code field} should be populated.
     *
     * @param field the schema field to resolve
     * @return the decision
     */
    FieldDecision resolve(BoundField field);
}
