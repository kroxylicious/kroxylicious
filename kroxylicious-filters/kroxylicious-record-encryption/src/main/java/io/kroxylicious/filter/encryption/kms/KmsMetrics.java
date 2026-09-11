/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Metrics counting the attempts at, and outcomes of, KMS operations.
 */
public interface KmsMetrics {
    /**
     * The outcome of a KMS operation.
     */
    enum OperationOutcome {
        /** The operation succeeded. */
        SUCCESS,
        /** The operation failed with an unexpected exception. */
        EXCEPTION,
        /** The operation failed because the key or alias was not found. */
        NOT_FOUND
    }

    /**
     * Counts an attempt to generate a DEK pair.
     */
    void countGenerateDekPairAttempt();

    /**
     * Counts the outcome of an attempt to generate a DEK pair.
     * @param outcome the outcome of the operation.
     */
    void countGenerateDekPairOutcome(@NonNull OperationOutcome outcome);

    /**
     * Counts an attempt to decrypt an encrypted DEK.
     */
    void countDecryptEdekAttempt();

    /**
     * Counts the outcome of an attempt to decrypt an encrypted DEK.
     * @param outcome the outcome of the operation.
     */
    void countDecryptEdekOutcome(@NonNull OperationOutcome outcome);

    /**
     * Counts an attempt to resolve an alias.
     */
    void countResolveAliasAttempt();

    /**
     * Counts the outcome of an attempt to resolve an alias.
     * @param outcome the outcome of the operation.
     */
    void countResolveAliasOutcome(@NonNull OperationOutcome outcome);
}
