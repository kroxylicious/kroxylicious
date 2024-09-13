/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.util.concurrent.CompletionStage;

import javax.crypto.SecretKey;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;

public class InstrumentedKms<K, E> implements Kms<K, E> {
    private final Kms<K, E> delegate;
    private final KmsMetrics metrics;

    private InstrumentedKms(Kms<K, E> delegate, KmsMetrics metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    public static <A, B> Kms<A, B> wrap(Kms<A, B> kms, KmsMetrics metrics) {
        return new InstrumentedKms<>(kms, metrics);
    }

    @NonNull
    @Override
    public CompletionStage<DekPair<E>> generateDekPair(
            @NonNull
            K kekRef
    ) {
        metrics.countGenerateDekPairAttempt();
        return delegate.generateDekPair(kekRef).whenComplete((eDekPair, throwable) -> {
            KmsMetrics.OperationOutcome outcome = classify(throwable);
            metrics.countGenerateDekPairOutcome(outcome);
        });
    }

    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(
            @NonNull
            E edek
    ) {
        metrics.countDecryptEdekAttempt();
        return delegate.decryptEdek(edek).whenComplete((eDekPair, throwable) -> {
            KmsMetrics.OperationOutcome outcome = classify(throwable);
            metrics.countDecryptEdekOutcome(outcome);
        });
    }

    @NonNull
    @Override
    public CompletionStage<K> resolveAlias(
            @NonNull
            String alias
    ) {
        metrics.countResolveAliasAttempt();
        return delegate.resolveAlias(alias).whenComplete((eDekPair, throwable) -> {
            KmsMetrics.OperationOutcome outcome = classify(throwable);
            metrics.countResolveAliasOutcome(outcome);
        });
    }

    @NonNull
    @Override
    public Serde<E> edekSerde() {
        return delegate.edekSerde();
    }

    private KmsMetrics.OperationOutcome classify(Throwable throwable) {
        if (throwable == null) {
            return KmsMetrics.OperationOutcome.SUCCESS;
        } else {
            if (isNotFoundException(throwable) || isNotFoundException(throwable.getCause())) {
                return KmsMetrics.OperationOutcome.NOT_FOUND;
            } else {
                return KmsMetrics.OperationOutcome.EXCEPTION;
            }
        }
    }

    private static boolean isNotFoundException(Throwable throwable) {
        return throwable instanceof UnknownKeyException || throwable instanceof UnknownAliasException;
    }
}
