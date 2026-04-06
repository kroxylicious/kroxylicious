/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

/**
 * Common keys for structured logging in kroxylicious-record-validation.
 */
public class RecordValidationLoggingKeys {

    private RecordValidationLoggingKeys() {
    }

    /**
     * Result of record validation.
     */
    public static final String VALIDATION_RESULT = "validationResult";
}
