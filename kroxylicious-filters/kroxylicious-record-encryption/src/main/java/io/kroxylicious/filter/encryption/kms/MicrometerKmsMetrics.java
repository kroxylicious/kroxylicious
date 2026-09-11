/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link KmsMetrics} implementation which publishes counters to a Micrometer registry.
 */
public class MicrometerKmsMetrics implements KmsMetrics {
    /** The prefix of the KMS operation meter names. */
    public static final String KMS_OPERATION_PREFIX = "kroxylicious_kms_operation";
    /** The name of the counter of KMS operation attempts. */
    public static final String ATTEMPT_COUNTER_NAME = KMS_OPERATION_PREFIX + "_attempt_total";
    /** The name of the counter of KMS operation outcomes. */
    public static final String OUTCOME_COUNTER_NAME = KMS_OPERATION_PREFIX + "_outcome_total";
    /** The key of the tag identifying the KMS operation. */
    public static final String OPERATION_TAG_KEY = "operation";
    /** The tag identifying the generate DEK pair operation. */
    public static final Tag OPERATION_GENERATE_DEK_PAIR_TAG = Tag.of(OPERATION_TAG_KEY, "generate_dek_pair");
    /** The tag identifying the decrypt encrypted DEK operation. */
    public static final Tag OPERATION_DECRYPT_EDEK_TAG = Tag.of(OPERATION_TAG_KEY, "decrypt_edek");
    /** The tag identifying the resolve alias operation. */
    public static final Tag OPERATION_RESOLVE_ALIAS_TAG = Tag.of(OPERATION_TAG_KEY, "resolve_alias");
    /** The key of the tag identifying the operation outcome. */
    public static final String OUTCOME_TAG_KEY = "outcome";
    /** The tag identifying the success outcome. */
    public static final Tag SUCCESS_OUTCOME_TAG = Tag.of(OUTCOME_TAG_KEY, "success");
    /** The tag identifying the not-found outcome. */
    public static final Tag NOT_FOUND_OUTCOME_TAG = Tag.of(OUTCOME_TAG_KEY, "not_found");
    /** The tag identifying the exception outcome. */
    public static final Tag EXCEPTION_OUTCOME_TAG = Tag.of(OUTCOME_TAG_KEY, "exception");
    private final Counter generateDekPairAttempts;
    private final Counter generateDekPairSuccesses;
    private final Counter generateDekPairExceptions;
    private final Counter generateDekPairNotFounds;
    private final Counter decryptEdekAttempts;
    private final Counter decryptEdekSuccesses;
    private final Counter decryptEdekNotFounds;
    private final Counter decryptEdekExceptions;
    private final Counter resolveAliasAttempt;
    private final Counter resolveAliasSuccesses;
    private final Counter resolveAliasNotFounds;
    private final Counter resolveAliasExceptions;

    private MicrometerKmsMetrics(MeterRegistry registry) {
        generateDekPairAttempts = attemptCounterForOperation(registry, OPERATION_GENERATE_DEK_PAIR_TAG);
        generateDekPairSuccesses = outcomeCounterForOperation(registry, OPERATION_GENERATE_DEK_PAIR_TAG, SUCCESS_OUTCOME_TAG);
        generateDekPairNotFounds = outcomeCounterForOperation(registry, OPERATION_GENERATE_DEK_PAIR_TAG, NOT_FOUND_OUTCOME_TAG);
        generateDekPairExceptions = outcomeCounterForOperation(registry, OPERATION_GENERATE_DEK_PAIR_TAG, EXCEPTION_OUTCOME_TAG);

        decryptEdekAttempts = attemptCounterForOperation(registry, OPERATION_DECRYPT_EDEK_TAG);
        decryptEdekSuccesses = outcomeCounterForOperation(registry, OPERATION_DECRYPT_EDEK_TAG, SUCCESS_OUTCOME_TAG);
        decryptEdekNotFounds = outcomeCounterForOperation(registry, OPERATION_DECRYPT_EDEK_TAG, NOT_FOUND_OUTCOME_TAG);
        decryptEdekExceptions = outcomeCounterForOperation(registry, OPERATION_DECRYPT_EDEK_TAG, EXCEPTION_OUTCOME_TAG);

        resolveAliasAttempt = attemptCounterForOperation(registry, OPERATION_RESOLVE_ALIAS_TAG);
        resolveAliasSuccesses = outcomeCounterForOperation(registry, OPERATION_RESOLVE_ALIAS_TAG, SUCCESS_OUTCOME_TAG);
        resolveAliasNotFounds = outcomeCounterForOperation(registry, OPERATION_RESOLVE_ALIAS_TAG, NOT_FOUND_OUTCOME_TAG);
        resolveAliasExceptions = outcomeCounterForOperation(registry, OPERATION_RESOLVE_ALIAS_TAG, EXCEPTION_OUTCOME_TAG);
    }

    /**
     * Creates KMS metrics publishing to the given registry.
     * @param registry the registry to which the counters are published.
     * @return KMS metrics publishing to the given registry.
     */
    public static KmsMetrics create(MeterRegistry registry) {
        return new MicrometerKmsMetrics(registry);
    }

    private Counter outcomeCounterForOperation(MeterRegistry registry, Tag operationTag, Tag outcome) {
        return registry.counter(OUTCOME_COUNTER_NAME, List.of(operationTag, outcome));
    }

    @NonNull
    private static Counter attemptCounterForOperation(MeterRegistry registry, Tag operationTag) {
        return registry.counter(ATTEMPT_COUNTER_NAME, List.of(operationTag));
    }

    @Override
    public void countGenerateDekPairAttempt() {
        generateDekPairAttempts.increment();
    }

    @Override
    public void countGenerateDekPairOutcome(@NonNull OperationOutcome outcome) {
        switch (outcome) {
            case SUCCESS -> generateDekPairSuccesses.increment();
            case EXCEPTION -> generateDekPairExceptions.increment();
            case NOT_FOUND -> generateDekPairNotFounds.increment();
        }
    }

    @Override
    public void countDecryptEdekAttempt() {
        decryptEdekAttempts.increment();
    }

    @Override
    public void countDecryptEdekOutcome(@NonNull OperationOutcome outcome) {
        switch (outcome) {
            case SUCCESS -> decryptEdekSuccesses.increment();
            case EXCEPTION -> decryptEdekExceptions.increment();
            case NOT_FOUND -> decryptEdekNotFounds.increment();
        }
    }

    @Override
    public void countResolveAliasAttempt() {
        resolveAliasAttempt.increment();
    }

    @Override
    public void countResolveAliasOutcome(@NonNull OperationOutcome outcome) {
        switch (outcome) {
            case SUCCESS -> resolveAliasSuccesses.increment();
            case EXCEPTION -> resolveAliasExceptions.increment();
            case NOT_FOUND -> resolveAliasNotFounds.increment();
        }
    }
}
