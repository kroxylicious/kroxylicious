/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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

import static io.kroxylicious.filter.encryption.KmsMetrics.OperationOutcome.EXCEPTION;
import static io.kroxylicious.filter.encryption.KmsMetrics.OperationOutcome.NOT_FOUND;
import static io.kroxylicious.filter.encryption.KmsMetrics.OperationOutcome.SUCCESS;
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
    public void testResolveAliasSuccess() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.resolveAlias("alias")).thenReturn(CompletableFuture.completedFuture("resolved"));
        CompletionStage<String> stage = instrument.resolveAlias("alias");
        assertThat(stage).succeedsWithin(Duration.ZERO);
        verify(metrics).countResolveAliasAttempt();
        verify(metrics).countResolveAliasOutcome(SUCCESS);
    }

    @Test
    public void testResolveAliasException() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.resolveAlias("alias")).thenReturn(CompletableFuture.failedFuture(new NullPointerException("fail")));
        CompletionStage<String> stage = instrument.resolveAlias("alias");
        assertThat(stage).failsWithin(Duration.ZERO);
        verify(metrics).countResolveAliasAttempt();
        verify(metrics).countResolveAliasOutcome(EXCEPTION);
    }

    @Test
    public void testResolveAliasUnknownAliasException() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.resolveAlias("alias")).thenReturn(CompletableFuture.failedFuture(new UnknownAliasException("unknown")));
        CompletionStage<String> stage = instrument.resolveAlias("alias");
        assertThat(stage).failsWithin(Duration.ZERO);
        verify(metrics).countResolveAliasAttempt();
        verify(metrics).countResolveAliasOutcome(NOT_FOUND);
    }

    @Test
    public void testGenerateDekPairSuccess() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        DekPair<String> dekPair = new DekPair<>("edek", secretKey);
        when(kms.generateDekPair("kekRef")).thenReturn(CompletableFuture.completedFuture(dekPair));
        CompletionStage<DekPair<String>> stage = instrument.generateDekPair("kekRef");
        assertThat(stage).succeedsWithin(Duration.ZERO).isSameAs(dekPair);
        verify(metrics).countGenerateDekPairAttempt();
        verify(metrics).countGenerateDekPairOutcome(SUCCESS);
    }

    @Test
    public void testGenerateDekPairException() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.generateDekPair("kekRef")).thenReturn(CompletableFuture.failedFuture(new NullPointerException("fail")));
        CompletionStage<DekPair<String>> stage = instrument.generateDekPair("kekRef");
        assertThat(stage).failsWithin(Duration.ZERO);
        verify(metrics).countGenerateDekPairAttempt();
        verify(metrics).countGenerateDekPairOutcome(EXCEPTION);
    }

    @Test
    public void testGenerateDekPairUnknownKeyException() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.generateDekPair("kekRef")).thenReturn(CompletableFuture.failedFuture(new UnknownKeyException("unknown")));
        CompletionStage<DekPair<String>> stage = instrument.generateDekPair("kekRef");
        assertThat(stage).failsWithin(Duration.ZERO);
        verify(metrics).countGenerateDekPairAttempt();
        verify(metrics).countGenerateDekPairOutcome(NOT_FOUND);
    }

    @Test
    public void testDecryptEdekSuccess() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.decryptEdek("edek")).thenReturn(CompletableFuture.completedFuture(secretKey));
        CompletionStage<SecretKey> stage = instrument.decryptEdek("edek");
        assertThat(stage).succeedsWithin(Duration.ZERO).isSameAs(secretKey);
        verify(metrics).countDecryptEdekAttempt();
        verify(metrics).countDecryptEdekOutcome(SUCCESS);
    }

    @Test
    public void testDecryptEdekException() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.decryptEdek("edek")).thenReturn(CompletableFuture.failedFuture(new NullPointerException("fail")));
        CompletionStage<SecretKey> stage = instrument.decryptEdek("edek");
        assertThat(stage).failsWithin(Duration.ZERO);
        verify(metrics).countDecryptEdekAttempt();
        verify(metrics).countDecryptEdekOutcome(EXCEPTION);
    }

    @Test
    public void testDecryptEdekUnknownKeyException() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.decryptEdek("edek")).thenReturn(CompletableFuture.failedFuture(new UnknownKeyException("unknown")));
        CompletionStage<SecretKey> stage = instrument.decryptEdek("edek");
        assertThat(stage).failsWithin(Duration.ZERO);
        verify(metrics).countDecryptEdekAttempt();
        verify(metrics).countDecryptEdekOutcome(NOT_FOUND);
    }

    @Test
    public void testEdekSerdeDelegation() {
        Kms<String, String> instrument = InstrumentedKms.instrument(kms, metrics);
        when(kms.edekSerde()).thenReturn(serde);
        Serde<String> serde = instrument.edekSerde();
        assertThat(serde).isSameAs(this.serde);
    }

}
