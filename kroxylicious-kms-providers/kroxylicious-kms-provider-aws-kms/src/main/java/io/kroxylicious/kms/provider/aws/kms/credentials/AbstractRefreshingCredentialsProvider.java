/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.slf4j.Logger;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Base class for {@link CredentialsProvider}s that obtain temporary credentials from a remote
 * endpoint and need to keep them refreshed in the background before expiration.
 * <p>
 * The provider will keep returning the same credential until the credential reaches a configured
 * factor of its lifespan.  At that point, a preemptive background refresh of the credential is
 * performed.  Until the refresh is complete the caller will continue to receive the existing
 * credential.  Once the refresh is complete subsequent calls will see the updated credential.
 * </p>
 * <p>
 * If an error occurs whilst retrieving the credential, the next call will cause the provider
 * to try again.  An exponential backoff (with jitter) is applied to retry attempts.
 * </p>
 *
 * @param <C> the concrete credential type produced by this provider
 */
abstract class AbstractRefreshingCredentialsProvider<C extends Credentials> implements CredentialsProvider {

    static final double DEFAULT_CREDENTIALS_LIFETIME_FACTOR = 0.80;

    private final Logger logger;
    private final Clock systemClock;
    private final AtomicReference<CompletableFuture<C>> current = new AtomicReference<>();
    private final AtomicLong refreshErrorCount = new AtomicLong();
    private final ScheduledExecutorService executorService;
    private final ExponentialBackoff backoff;
    private final double lifetimeFactor;

    @SuppressWarnings("java:S2245") // Pseudorandomness sufficient for generating backoff jitter; not security relevant
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Pseudorandomness sufficient for generating backoff jitter; not security relevant
    protected AbstractRefreshingCredentialsProvider(String threadName,
                                                    Logger logger,
                                                    Clock systemClock,
                                                    double lifetimeFactor) {
        Objects.requireNonNull(threadName);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(systemClock);
        this.logger = logger;
        this.systemClock = systemClock;
        this.lifetimeFactor = lifetimeFactor;
        this.executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(r, threadName);
            thread.setDaemon(true);
            return thread;
        });
        this.backoff = new ExponentialBackoff(500, 2, 60000, ThreadLocalRandom.current().nextDouble());
    }

    /**
     * Subclass hook performing the actual HTTP-level fetch of a fresh credential.  Invoked on
     * the provider's scheduled-executor thread.
     *
     * @return a stage that completes with the new credential
     */
    protected abstract CompletionStage<C> fetchCredentials();

    /**
     * @param credentials a credential previously returned by {@link #fetchCredentials()}
     * @return the instant at which the credential will expire
     */
    protected abstract Instant expirationOf(C credentials);

    /**
     * Hook invoked after a refresh attempt fails.  The default implementation logs at WARN.
     * Subclasses may override to add provider-specific log keys.
     *
     * @param t the failure
     */
    protected void onRefreshFailure(Throwable t) {
        logger.atWarn()
                .setCause(logger.isDebugEnabled() ? t : null)
                .addKeyValue("error", t.getMessage())
                .log(logger.isDebugEnabled()
                        ? "refresh of AWS credentials failed"
                        : "refresh of AWS credentials failed; increase log level to DEBUG for stacktrace");
    }

    /**
     * Hook invoked after a refresh attempt succeeds.  The default implementation logs at DEBUG.
     * Subclasses may override to add provider-specific log keys.
     *
     * @param credentials the new credential
     */
    protected void onRefreshSuccess(C credentials) {
        logger.atDebug()
                .addKeyValue("expiration", expirationOf(credentials))
                .log("Obtained AWS credentials");
    }

    @Override
    public CompletionStage<? extends C> getCredentials() {
        var newCredFuture = new CompletableFuture<C>();
        var witness = current.compareAndExchange(null, newCredFuture);
        if (witness == null) {
            // there's no current credential, let's create one
            executorService.execute(() -> refreshCredential(newCredFuture));
            return newCredFuture.minimalCompletionStage();
        }
        else if (isExpired(witness) || witness.isCompletedExceptionally()) {
            // current credential is expired, or it has been completed exceptionally.
            // throw it away and generate a new one.
            // we don't normally expect to follow the expired path as the preemptive refresh ought
            // to have caused its refresh before its expiration.
            current.compareAndSet(witness, null);
            return getCredentials();
        }
        return witness.minimalCompletionStage();
    }

    private void refreshCredential(CompletableFuture<C> future) {
        try {
            fetchCredentials().whenComplete((credentials, t) -> propagateResultToFuture(credentials, t, future));
        }
        catch (RuntimeException e) {
            propagateResultToFuture(null, e, future);
        }
    }

    private void scheduleCredentialRefresh(long delay) {
        logger.atDebug()
                .addKeyValue("delayMs", delay)
                .log("Scheduling refresh of AWS credentials");

        var refreshedCredFuture = new CompletableFuture<C>();
        executorService.schedule(() -> {
            refreshCredential(refreshedCredFuture);
            refreshedCredFuture.thenApply(sc -> {
                var previous = current.getAndSet(refreshedCredFuture);
                // the previous future has typically already completed, but for safety, complete it anyway.
                Optional.ofNullable(previous).ifPresent(f -> f.complete(sc));
                return null;
            });
        }, delay, TimeUnit.MILLISECONDS);
    }

    private boolean isExpired(CompletableFuture<C> witness) {
        if (witness.isDone() && !witness.isCompletedExceptionally()) {
            try {
                return Optional.ofNullable(witness.getNow(null))
                        .map(this::expirationOf)
                        .map(exp -> systemClock.instant().isAfter(exp))
                        .orElse(false);
            }
            catch (CancellationException | CompletionException e) {
                return false;
            }
        }
        return false;
    }

    private void propagateResultToFuture(@Nullable C credentials, @Nullable Throwable t, CompletableFuture<C> target) {
        final long refreshDelay;
        if (t != null) {
            onRefreshFailure(t);
            refreshErrorCount.incrementAndGet();
            target.completeExceptionally(t);
            refreshDelay = backoff.backoff(refreshErrorCount.get());
        }
        else {
            // when t == null, fetchCredentials() must have produced a non-null credential
            Objects.requireNonNull(credentials);
            onRefreshSuccess(credentials);
            refreshErrorCount.set(0);
            target.complete(credentials);
            var expiration = expirationOf(credentials);
            refreshDelay = (long) Math.max(0, this.lifetimeFactor * (expiration.toEpochMilli() - systemClock.instant().toEpochMilli()));
        }
        scheduleCredentialRefresh(refreshDelay);
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }
}
