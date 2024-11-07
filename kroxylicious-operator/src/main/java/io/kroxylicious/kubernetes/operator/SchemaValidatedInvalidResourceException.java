/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

/**
 * This should be used for error cases which should be validated by the CRD schema (including CEL validation rules).
 * I.e. Situations which "ought to be impossible", but where we're writing code to handle it anyway.
 * It is intended to simplify correlating with the code with schema.
 */
public class SchemaValidatedInvalidResourceException extends InvalidResourceException {

    public SchemaValidatedInvalidResourceException(String message) {
        super(message);
    }
}
