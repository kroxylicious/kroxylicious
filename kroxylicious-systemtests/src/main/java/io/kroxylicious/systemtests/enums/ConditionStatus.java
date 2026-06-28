/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.enums;

/**
 * Status of the CR, found inside .status.conditions.*.status
 */
public enum ConditionStatus {
    /**
     *True condition status.
     */
    TRUE,
    /**
     *False condition status.
     */
    FALSE
}
