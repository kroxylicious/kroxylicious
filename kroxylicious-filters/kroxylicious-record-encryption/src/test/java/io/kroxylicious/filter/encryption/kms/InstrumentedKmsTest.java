/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import javax.crypto.SecretKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static io.kroxylicious.filter.encryption.kms.KmsMetrics.OperationOutcome.EXCEPTION;
import static io.kroxylicious.filter.encryption.kms.KmsMetrics.OperationOutcome.NOT_FOUND;
import static io.kroxylicious.filter.encryption.kms.KmsMetrics.OperationOutcome.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InstrumentedKmsTest {

    @Mock
    Kms<String, String> kms;
    @Mock
    KmsMetrics metrics;
    @Mock
    SecretKey secretKey;
    @Mock
    Serde<String> serde;

    @Test
    void testResolveAliasSuccess() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        when(kms.resolveAlias("alias")).thenReturn(CompletableFuture.completedFuture("resolved"));
        CompletionStage<String> stage = instrument.resolveAlias("alias");
        assertThat(stage).succeedsWithin(Duration.ZERO).isEqualTo("resolved");
        verify(metrics).countResolveAliasAttempt();
        verify(metrics).countResolveAliasOutcome(SUCCESS);
    }

    @Test
    void testResolveAliasException() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        NullPointerException cause = new NullPointerException("fail");
        when(kms.resolveAlias("alias")).thenReturn(CompletableFuture.failedFuture(cause));
        CompletionStage<String> stage = instrument.resolveAlias("alias");
        assertStageFailsWithCause(stage, cause);
        verify(metrics).countResolveAliasAttempt();
        verify(metrics).countResolveAliasOutcome(EXCEPTION);
    }

    @Test
    void testResolveAliasUnknownAliasException() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        UnknownAliasException cause = new UnknownAliasException("unknown");
        when(kms.resolveAlias("alias")).thenReturn(CompletableFuture.failedFuture(cause));
        CompletionStage<String> stage = instrument.resolveAlias("alias");
        assertStageFailsWithCause(stage, cause);
        verify(metrics).countResolveAliasAttempt();
        verify(metrics).countResolveAliasOutcome(NOT_FOUND);
    }

    @Test
    void testGenerateDekPairSuccess() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        DekPair<String> dekPair = new DekPair<>("edek", secretKey);
        when(kms.generateDekPair("kekRef")).thenReturn(CompletableFuture.completedFuture(dekPair));
        CompletionStage<DekPair<String>> stage = instrument.generateDekPair("kekRef");
        assertThat(stage).succeedsWithin(Duration.ZERO).isSameAs(dekPair);
        verify(metrics).countGenerateDekPairAttempt();
        verify(metrics).countGenerateDekPairOutcome(SUCCESS);
    }

    @Test
    void testGenerateDekPairException() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        NullPointerException cause = new NullPointerException("fail");
        when(kms.generateDekPair("kekRef")).thenReturn(CompletableFuture.failedFuture(cause));
        CompletionStage<DekPair<String>> stage = instrument.generateDekPair("kekRef");
        assertStageFailsWithCause(stage, cause);
        verify(metrics).countGenerateDekPairAttempt();
        verify(metrics).countGenerateDekPairOutcome(EXCEPTION);
    }

    @Test
    void testGenerateDekPairUnknownKeyException() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        UnknownKeyException cause = new UnknownKeyException("unknown");
        when(kms.generateDekPair("kekRef")).thenReturn(CompletableFuture.failedFuture(cause));
        CompletionStage<DekPair<String>> stage = instrument.generateDekPair("kekRef");
        assertStageFailsWithCause(stage, cause);
        verify(metrics).countGenerateDekPairAttempt();
        verify(metrics).countGenerateDekPairOutcome(NOT_FOUND);
    }

    @Test
    void testDecryptEdekSuccess() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        when(kms.decryptEdek("edek")).thenReturn(CompletableFuture.completedFuture(secretKey));
        CompletionStage<SecretKey> stage = instrument.decryptEdek("edek");
        assertThat(stage).succeedsWithin(Duration.ZERO).isSameAs(secretKey);
        verify(metrics).countDecryptEdekAttempt();
        verify(metrics).countDecryptEdekOutcome(SUCCESS);
    }

    @Test
    void testDecryptEdekException() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        NullPointerException cause = new NullPointerException("fail");
        when(kms.decryptEdek("edek")).thenReturn(CompletableFuture.failedFuture(cause));
        CompletionStage<SecretKey> stage = instrument.decryptEdek("edek");
        assertStageFailsWithCause(stage, cause);
        verify(metrics).countDecryptEdekAttempt();
        verify(metrics).countDecryptEdekOutcome(EXCEPTION);
    }

    @Test
    void testDecryptEdekUnknownKeyException() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        UnknownKeyException cause = new UnknownKeyException("unknown");
        when(kms.decryptEdek("edek")).thenReturn(CompletableFuture.failedFuture(cause));
        CompletionStage<SecretKey> stage = instrument.decryptEdek("edek");
        assertStageFailsWithCause(stage, cause);
        verify(metrics).countDecryptEdekAttempt();
        verify(metrics).countDecryptEdekOutcome(NOT_FOUND);
    }

    private static void assertStageFailsWithCause(CompletionStage<?> stage, Throwable cause) {
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).withCause(cause);
    }

    @Test
    void testEdekSerdeDelegation() {
        Kms<String, String> instrument = InstrumentedKms.wrap(kms, metrics);
        when(kms.edekSerde()).thenReturn(serde);
        Serde<String> edekSerde = instrument.edekSerde();
        assertThat(edekSerde).isSameAs(this.serde);
    }

}
