/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerKmsMetricsTest {
    public static final String KMS_OPERATION_PREFIX = "kroxylicious_kms_operation";
    public static final String ATTEMPT_COUNTER_NAME = KMS_OPERATION_PREFIX + "_attempt_total";
    public static final String OUTCOME_COUNTER_NAME = KMS_OPERATION_PREFIX + "_outcome_total";
    public static final String OPERATION_TAG_KEY = "operation";
    public static final Tag OPERATION_GENERATE_DEK_PAIR_TAG = Tag.of(OPERATION_TAG_KEY, "generate_dek_pair");
    public static final Tag OPERATION_DECRYPT_EDEK_TAG = Tag.of(OPERATION_TAG_KEY, "decrypt_edek");
    public static final Tag OPERATION_RESOLVE_ALIAS_TAG = Tag.of(OPERATION_TAG_KEY, "resolve_alias");
    public static final String OUTCOME_TAG_KEY = "outcome";
    public static final Tag SUCCESS_OUTCOME_TAG = Tag.of(OUTCOME_TAG_KEY, "success");
    public static final Tag NOT_FOUND_OUTCOME_TAG = Tag.of(OUTCOME_TAG_KEY, "not_found");
    public static final Tag EXCEPTION_OUTCOME_TAG = Tag.of(OUTCOME_TAG_KEY, "exception");
    private SimpleMeterRegistry registry = new SimpleMeterRegistry();
    KmsMetrics kmsMetrics = MicrometerKmsMetrics.create(registry);

    @AfterEach
    void teardown() {
        registry.close();
    }

    @Test
    void testCountDecryptEdekAttempt() {
        kmsMetrics.countDecryptEdekAttempt();
        assertCounterValueEquals(ATTEMPT_COUNTER_NAME, List.of(OPERATION_DECRYPT_EDEK_TAG), 1.0d);
    }

    @Test
    void testCountDecryptEdekSuccess() {
        kmsMetrics.countDecryptEdekOutcome(KmsMetrics.OperationOutcome.SUCCESS);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_DECRYPT_EDEK_TAG, SUCCESS_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountDecryptEdekNotFound() {
        kmsMetrics.countDecryptEdekOutcome(KmsMetrics.OperationOutcome.NOT_FOUND);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_DECRYPT_EDEK_TAG, NOT_FOUND_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountDecryptEdekException() {
        kmsMetrics.countDecryptEdekOutcome(KmsMetrics.OperationOutcome.EXCEPTION);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_DECRYPT_EDEK_TAG, EXCEPTION_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountGenerateDekPairAttempt() {
        kmsMetrics.countGenerateDekPairAttempt();
        assertCounterValueEquals(ATTEMPT_COUNTER_NAME, List.of(OPERATION_GENERATE_DEK_PAIR_TAG), 1.0d);
    }

    @Test
    void testCountGenerateDekPairSuccess() {
        kmsMetrics.countGenerateDekPairOutcome(KmsMetrics.OperationOutcome.SUCCESS);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_GENERATE_DEK_PAIR_TAG, SUCCESS_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountGenerateDekPairNotFound() {
        kmsMetrics.countGenerateDekPairOutcome(KmsMetrics.OperationOutcome.NOT_FOUND);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_GENERATE_DEK_PAIR_TAG, NOT_FOUND_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountGenerateDekPairException() {
        kmsMetrics.countGenerateDekPairOutcome(KmsMetrics.OperationOutcome.EXCEPTION);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_GENERATE_DEK_PAIR_TAG, EXCEPTION_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountResolveAliasAttempt() {
        kmsMetrics.countResolveAliasAttempt();
        assertCounterValueEquals(ATTEMPT_COUNTER_NAME, List.of(OPERATION_RESOLVE_ALIAS_TAG), 1.0d);
    }

    @Test
    void testCountResolveAliasSuccess() {
        kmsMetrics.countResolveAliasOutcome(KmsMetrics.OperationOutcome.SUCCESS);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_RESOLVE_ALIAS_TAG, SUCCESS_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountResolveAliasNotFound() {
        kmsMetrics.countResolveAliasOutcome(KmsMetrics.OperationOutcome.NOT_FOUND);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_RESOLVE_ALIAS_TAG, NOT_FOUND_OUTCOME_TAG), 1.0d);
    }

    @Test
    void testCountResolveAliasException() {
        kmsMetrics.countResolveAliasOutcome(KmsMetrics.OperationOutcome.EXCEPTION);
        assertCounterValueEquals(OUTCOME_COUNTER_NAME, List.of(OPERATION_RESOLVE_ALIAS_TAG, EXCEPTION_OUTCOME_TAG), 1.0d);
    }

    private void assertCounterValueEquals(String name, Iterable<Tag> tags, double expected) {
        Counter counter = registry.find(name).tags(tags).counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(expected);
    }

}
