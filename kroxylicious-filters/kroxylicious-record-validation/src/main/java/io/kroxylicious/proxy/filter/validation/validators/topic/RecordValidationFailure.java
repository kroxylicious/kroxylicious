/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.topic;

/**
 * Details about an invalid record from a topic-partition
 * @param invalidIndex the index of the invalid record within its batch
 * @param errorMessage details of what was invalid
 */
public record RecordValidationFailure(
        int invalidIndex,
        String errorMessage
) {

    @Override
    public String toString() {
        return "(" + invalidIndex + ", " + errorMessage + ")";
    }
}
