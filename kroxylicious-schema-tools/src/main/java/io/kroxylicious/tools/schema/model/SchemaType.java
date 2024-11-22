/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum SchemaType {
    @JsonProperty("null")
    NULL,
    @JsonProperty("boolean")
    BOOLEAN,
    @JsonProperty("integer")
    INTEGER,
    @JsonProperty("number")
    NUMBER,
    @JsonProperty("string")
    STRING,
    @JsonProperty("array")
    ARRAY,
    @JsonProperty("object")
    OBJECT;

    public static List<SchemaType> all() {
        return Arrays.asList(SchemaType.values());
    }
}
