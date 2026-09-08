/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.lang.reflect.Method;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;

/**
 * Walks Kafka's authoritative runtime protocol schema for a message and drives the configured
 * {@link FieldPopulationStrategy} over each field, invoking the corresponding generated setter on
 * whichever {@code *Data} instance ({@code io.kroxylicious.*} or {@code org.apache.kafka.*}) was handed in.
 * <p>
 * Kafka's schema, not Kroxylicious's, is authoritative here: the fidelity being proven is that
 * Kroxylicious's generated classes match Kafka's wire behaviour, so population must be driven by
 * Kafka's own understanding of each field's type.
 */
public final class SchemaDrivenMessagePopulator implements MessagePopulator {

    private final FieldPopulationStrategy strategy;

    /**
     * @param strategy decides the value, if any, for each field visited
     */
    public SchemaDrivenMessagePopulator(FieldPopulationStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public PopulationResult populate(Object instance, ApiKeys apiKey, short version) {
        Schema schema = kafkaSchemaFor(instance, version);
        for (BoundField field : schema.fields()) {
            FieldDecision decision = strategy.resolve(field);
            if (decision instanceof FieldDecision.Value value) {
                invokeSetter(instance, field, value.value());
            }
            else {
                throw new UnsupportedOperationException(
                        "Composite/array field walking is not yet supported: " + field.def.name);
            }
        }
        return new PopulationResult.Populated();
    }

    private static Schema kafkaSchemaFor(Object instance, short version) {
        String kafkaClassName = "org.apache.kafka.common.message." + instance.getClass().getSimpleName();
        try {
            Class<?> kafkaClass = Class.forName(kafkaClassName);
            Schema[] schemas = (Schema[]) kafkaClass.getField("SCHEMAS").get(null);
            return schemas[version];
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Could not resolve Kafka schema for " + kafkaClassName, e);
        }
    }

    private static void invokeSetter(Object instance, BoundField field, Object value) {
        String setterName = "set" + Character.toUpperCase(field.def.name.charAt(0)) + field.def.name.substring(1);
        try {
            Method setter = instance.getClass().getMethod(setterName, String.class);
            setter.invoke(instance, value);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Could not invoke " + setterName + " on " + instance.getClass(), e);
        }
    }
}
