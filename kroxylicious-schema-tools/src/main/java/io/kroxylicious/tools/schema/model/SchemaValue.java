/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import java.util.Objects;

/**
 * A schema value is either a SchemaObject or a boolean.
 */
public class SchemaValue {
    private final Object value;

    public SchemaValue(SchemaObject object) {
        this.value = Objects.requireNonNull(object);
    }

    public SchemaValue(boolean value) {
        this.value = value;
    }

    SchemaObject object() {
        return (SchemaObject) value;
    }

    boolean bool() {
        return (Boolean) value;
    }
}
