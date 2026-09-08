/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import org.apache.kafka.common.protocol.types.BoundField;

/**
 * Identifies the schema field a {@link PopulationResult.Failed} population attempt failed on.
 *
 * @param field the schema field
 */
public record FieldPath(BoundField field) {}
