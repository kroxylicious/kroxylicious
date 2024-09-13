/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface KmsMetrics {
    enum OperationOutcome {
        SUCCESS,
        EXCEPTION,
        NOT_FOUND
    }

    void countGenerateDekPairAttempt();

    void countGenerateDekPairOutcome(
            @NonNull
            OperationOutcome outcome
    );

    void countDecryptEdekAttempt();

    void countDecryptEdekOutcome(
            @NonNull
            OperationOutcome outcome
    );

    void countResolveAliasAttempt();

    void countResolveAliasOutcome(
            @NonNull
            OperationOutcome outcome
    );
}
