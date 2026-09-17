/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

/**
 * Populates the fields of a generated {@code *Data} message instance with values, driven by Kafka's
 * runtime protocol schema rather than by reflecting over the instance's own declared field types.
 */
public interface MessagePopulator {

    /**
     * Populates every field of {@code instance} that the underlying schema walk visits.
     *
     * @param instance the message instance to populate; may be a Kroxylicious or a Kafka {@code *Data} instance
     * @param version the protocol version to populate fields for
     * @return the outcome of the population attempt
     */
    PopulationResult populate(Object instance, short version);
}
