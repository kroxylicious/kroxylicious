/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.crypto.SecretKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public class ResilientKms<K, E> implements Kms<K, E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResilientKms.class);
    private final Kms<K, E> inner;
    private final ScheduledExecutorService executorService;
    private final BackoffStrategy strategy;
    private final int retries;

    private ResilientKms(
            @NonNull
            Kms<K, E> inner,
            @NonNull
            ScheduledExecutorService executorService,
            @NonNull
            BackoffStrategy backoffStrategy,
            int retries
    ) {
        this.inner = requireNonNull(inner);
        this.executorService = requireNonNull(executorService);
        strategy = requireNonNull(backoffStrategy);
        this.retries = retries;
    }

    public static <K, E> Kms<K, E> wrap(
            @NonNull
            Kms<K, E> delegate,
            @NonNull
            ScheduledExecutorService executorService,
            @NonNull
            BackoffStrategy strategy,
            int retries
    ) {
        return new ResilientKms<>(
                delegate,
                executorService,
                strategy,
                retries
        );
    }

    @NonNull
    @Override
    public CompletionStage<DekPair<E>> generateDekPair(
            @NonNull
            K kekRef
    ) {
        return retry("generateDekPair", () -> inner.generateDekPair(kekRef));
    }

    @NonNull
    @Override
    public CompletionStage<SecretKey> decryptEdek(
            @NonNull
            E edek
    ) {
        return retry("decryptEdek", () -> inner.decryptEdek(edek));
    }

    @NonNull
    @Override
    public Serde edekSerde() {
        return inner.edekSerde();
    }

    @NonNull
    @Override
    public CompletionStage<K> resolveAlias(
            @NonNull
            String alias
    ) {
        return retry("resolveAlias", () -> inner.resolveAlias(alias));
    }

    public <A> CompletionStage<A> retry(String name, Supplier<CompletionStage<A>> operation) {
        return retry(name, operation, 0, null);
    }

    private <A> CompletionStage<A> retry(
            String name,
            Supplier<CompletionStage<A>> operation,
            int attempt,
            @Nullable
            Throwable lastFailure
    ) {
        if (attempt >= retries) {
            String lastFailureMessage = ofNullable(lastFailure).map(Throwable::getMessage).orElse("null");
            String message = name + " failed after " + attempt + " attempts, last failure message: " + lastFailureMessage;
            return CompletableFuture.failedFuture(new KmsException(message, lastFailure));
        }
        Duration delay = strategy.getDelay(attempt);
        return schedule(operation, delay)
                                         .exceptionallyCompose(e -> {
                                             if (isUnknownEntityException(e) || (e instanceof CompletionException ce && (isUnknownEntityException(ce.getCause())))) {
                                                 LOGGER.debug("not retrying unknown entity exception");
                                                 return CompletableFuture.failedFuture(e);
                                             }
                                             LOGGER.debug("{} failed attempt {}", name, attempt, e);
                                             return retry(name, operation, attempt + 1, e);
                                         });
    }

    private static boolean isUnknownEntityException(Throwable e) {
        return e instanceof UnknownAliasException || e instanceof UnknownKeyException;
    }

    private <A> CompletionStage<A> schedule(Supplier<CompletionStage<A>> operation, Duration duration) {
        if (duration.equals(Duration.ZERO)) {
            return operation.get();
        }
        CompletableFuture<A> future = new CompletableFuture<>();
        executorService.schedule(() -> {
            operation.get().whenComplete((a, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(a);
                }
            });
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
        return future;
    }
}
