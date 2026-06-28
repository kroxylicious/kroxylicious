/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.crypto.SecretKey;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

class AsyncKms<K, E> implements Kms<K, E> {
    private final Kms<K, E> delegate;
    private final Executor executor;

    AsyncKms(Kms<K, E> delegate, Executor executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @NonNull
    @Override
    public CompletionStage<DekPair<E>> generateDekPair(@NonNull K kekRef) {
        return completingOnExecutor(delegate.generateDekPair(kekRef));
    }

    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(@NonNull E edek) {
        return completingOnExecutor(delegate.decryptEdek(edek));
    }

    @NonNull
    @Override
    public Serde<E> edekSerde() {
        return delegate.edekSerde();
    }

    @NonNull
    @Override
    public CompletionStage<K> resolveAlias(@NonNull String alias) {
        return completingOnExecutor(delegate.resolveAlias(alias));
    }

    private <T> @NonNull CompletionStage<T> completingOnExecutor(@NonNull CompletionStage<T> stage) {
        return stage.whenCompleteAsync((t, throwable) -> {
        }, executor);
    }

}
