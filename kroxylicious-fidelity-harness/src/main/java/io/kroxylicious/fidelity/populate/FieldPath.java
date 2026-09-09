/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import org.apache.kafka.common.protocol.types.BoundField;

/**
 * Identifies the schema field a {@link PopulationResult.Failed} population attempt failed on.
 * <p>
 * Not constructed anywhere yet, for the same reason {@link PopulationResult.Failed} isn't: it
 * sketches the addressing scheme future error-parity fidelity checks will need, ahead of any
 * populator actually producing a {@code Failed} result.
 *
 * @param field the schema field
 */
public record FieldPath(BoundField field) {}
