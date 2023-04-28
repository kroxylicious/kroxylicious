/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation;

/**
 * Result for a validation
 * @param valid whether the input was valid
 * @param errorMessage error message that should be supplied when input is invalid
 */
public record Result(boolean valid, String errorMessage) {

    /**
     * valid result
     */
    public static Result VALID = new Result(true, null);
}
