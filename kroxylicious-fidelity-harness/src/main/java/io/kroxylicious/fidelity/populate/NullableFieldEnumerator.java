/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Schema;

/**
 * Lists the fields of a message that are legally nullable at a given protocol version, driven by Kafka's
 * runtime protocol schema in the same way {@link SchemaDrivenMessagePopulator} is - including fields
 * nested arbitrarily deep inside structs and arrays-of-structs.
 */
public final class NullableFieldEnumerator {

    private NullableFieldEnumerator() {
    }

    /**
     * Lists the nullable fields of {@code instance}'s message type at {@code version}, recursing into
     * nested structs and arrays-of-structs.
     *
     * @param instance a Kroxylicious or a Kafka {@code *Data} instance
     * @param version the protocol version to enumerate fields for
     * @return the fields whose type is nullable at {@code version}
     */
    public static List<BoundField> nullableFields(Object instance, short version) {
        Class<?> kafkaClass = SchemaDrivenMessagePopulator.kafkaClassFor(instance);
        return nullableFieldsOf(kafkaClass, kafkaClass, version).toList();
    }

    private static Stream<BoundField> nullableFieldsOf(Class<?> rootKafkaClass, Class<?> kafkaClass, short version) {
        Schema schema = SchemaDrivenMessagePopulator.kafkaSchemaFor(kafkaClass, version);
        return SchemaDrivenMessagePopulator.expandFields(schema)
                .flatMap(field -> nullableFieldsFrom(rootKafkaClass, field, version));
    }

    /**
     * Every field contributes itself, if nullable, and - for a struct or array-of-struct field -
     * whatever nullable fields recursing into the nested struct's own schema finds, regardless of
     * whether the struct field itself is nullable: nullability of the container and nullability of its
     * contents are independent facts.
     */
    private static Stream<BoundField> nullableFieldsFrom(Class<?> rootKafkaClass, BoundField field, short version) {
        Stream<BoundField> self = field.def.type.isNullable() ? Stream.of(field) : Stream.empty();
        Stream<BoundField> nested = SchemaDrivenMessagePopulator.structTypeOf(field.def.type)
                .flatMap(structSchema -> SchemaDrivenMessagePopulator.resolveStructClass(rootKafkaClass, structSchema))
                .map(structClass -> nullableFieldsOf(rootKafkaClass, structClass, version))
                .orElseGet(Stream::empty);
        return Stream.concat(self, nested);
    }
}
