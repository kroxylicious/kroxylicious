/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.List;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;

/**
 * Lists the top-level fields of a message that are legally nullable at a given protocol version, driven
 * by Kafka's runtime protocol schema in the same way {@link SchemaDrivenMessagePopulator} is.
 */
public final class NullableFieldEnumerator {

    private NullableFieldEnumerator() {
    }

    /**
     * Lists the top-level nullable fields of {@code instance}'s message type at {@code version}.
     *
     * @param instance a Kroxylicious or a Kafka {@code *Data} instance
     * @param version the protocol version to enumerate fields for
     * @return the top-level fields whose type is nullable at {@code version}
     */
    public static List<BoundField> topLevelNullableFields(Object instance, short version) {
        Class<?> kafkaClass = SchemaDrivenMessagePopulator.kafkaClassFor(instance);
        Schema schema = SchemaDrivenMessagePopulator.kafkaSchemaFor(kafkaClass, version);
        return SchemaDrivenMessagePopulator.expandFields(schema)
                .filter(field -> field.def.type.isNullable())
                .toList();
    }
}
