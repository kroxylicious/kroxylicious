/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum EntityType {
    @JsonProperty(
        "unknown"
    ) UNKNOWN(null),

    @JsonProperty(
        "transactionalId"
    ) TRANSACTIONAL_ID(FieldType.StringFieldType.INSTANCE),

    @JsonProperty(
        "producerId"
    ) PRODUCER_ID(FieldType.Int64FieldType.INSTANCE),

    @JsonProperty(
        "groupId"
    ) GROUP_ID(FieldType.StringFieldType.INSTANCE),

    @JsonProperty(
        "topicName"
    ) TOPIC_NAME(FieldType.StringFieldType.INSTANCE),

    @JsonProperty(
        "brokerId"
    ) BROKER_ID(FieldType.Int32FieldType.INSTANCE);

    private final FieldType baseType;

    EntityType(FieldType baseType) {
        this.baseType = baseType;
    }

    public void verifyTypeMatches(String fieldName, FieldType type) {
        if (this == UNKNOWN) {
            return;
        }
        if (type instanceof FieldType.ArrayType arrayType) {
            verifyTypeMatches(fieldName, arrayType.elementType());
        } else {
            if (!type.toString().equals(baseType.toString())) {
                throw new RuntimeException(
                        "Field "
                                           + fieldName
                                           + " has entity type "
                                           +
                                           name()
                                           + ", but field type "
                                           + type.toString()
                                           + ", which does "
                                           +
                                           "not match."
                );
            }
        }
    }
}
