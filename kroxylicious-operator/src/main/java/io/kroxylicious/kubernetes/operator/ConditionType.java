/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

public enum ConditionType {
    Ready("Ready"),
    Accepted("Accepted");

    private final String value;

    ConditionType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
