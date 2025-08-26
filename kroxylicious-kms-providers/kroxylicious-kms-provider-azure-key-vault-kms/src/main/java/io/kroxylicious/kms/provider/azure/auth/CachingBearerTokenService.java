/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kms.provider.azure.auth;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static java.time.temporal.ChronoUnit.MINUTES;

/**
 * BearerTokenService that caches tokens from a delegate.
 * Properties of the caching token service:
 * 1. There should only ever be one in-flight refresh operation obtaining a new token
 * 2. During a refresh, if we have a non-expired cached token, it is returned to clients
 * 3. After a refresh completes successfully, we discard the previously cached token
 * 4. If a client obtains a future from the Initial state, it will be completed after Refresh terminates, or if the service is closed
 * 5. If a client obtains a future from the Refreshing state, it will be completed after Refresh terminates, or if the service is closed
 * The implementation is threadsafe
 */
@ThreadSafe
public class CachingBearerTokenService implements BearerTokenService {

    private final BearerTokenService delegate;
    private final AtomicReference<State> state;
    private final Clock clock;
    private static final Logger LOG = LoggerFactory.getLogger(CachingBearerTokenService.class);

    public CachingBearerTokenService(BearerTokenService delegate, Clock clock) {
        this(delegate, new State.Initial(new CompletableFuture<>()), clock);
    }

    @VisibleForTesting
    CachingBearerTokenService(BearerTokenService delegate, State state, Clock clock) {
        Objects.requireNonNull(delegate, "delegate cannot be null");
        Objects.requireNonNull(state, "state cannot be null");
        Objects.requireNonNull(clock, "clock cannot be null");
        this.delegate = delegate;
        this.state = new AtomicReference<>(state);
        this.clock = clock;
    }

    sealed interface State permits State.Initial, State.Refreshing, State.Steady, State.Closed {

        CompletableFuture<BearerToken> tokenPromise();

        record Initial(CompletableFuture<BearerToken> tokenPromise) implements State {

            @Override
            public String toString() {
                return "Initial";
            }
        }

        record Refreshing(@Nullable BearerToken current, CompletableFuture<BearerToken> promise, Clock clock) implements State {
            public Refreshing {
                Objects.requireNonNull(promise, "promise cannot be null");
            }

            @Override
            public CompletableFuture<BearerToken> tokenPromise() {
                // during a refresh, use the current token while it is non-expired
                if (current != null && !current.isExpired(clock.instant())) {
                    return CompletableFuture.completedFuture(current);
                }
                // else return the promise, expecting it to be completed in future
                return promise;
            }

            @Override
            public String toString() {
                return "Refreshing";
            }
        }

        record Steady(BearerToken current) implements State {
            public Steady {
                Objects.requireNonNull(current, "current cannot be null");
            }

            @Override
            public String toString() {
                return "Steady";
            }

            @Override
            public CompletableFuture<BearerToken> tokenPromise() {
                return CompletableFuture.completedFuture(current);
            }
        }

        record Closed() implements State {

            @Override
            public String toString() {
                return "Closed";
            }

            @Override
            public CompletableFuture<BearerToken> tokenPromise() {
                return CompletableFuture.failedFuture(new IllegalStateException("service is closed"));
            }
        }
    }

    @Override
    public CompletionStage<BearerToken> getBearerToken() {
        State current = state.get();
        if (current instanceof State.Initial initial) {
            transitionToRefreshing(current, null, initial.tokenPromise());
        }
        else if (current instanceof State.Steady steady) {
            refreshIfNearExpiry(steady);
        }
        return current.tokenPromise();
    }

    private void refreshIfNearExpiry(State.Steady steady) {
        BearerToken currentToken = steady.current();
        if (clock.instant().plus(1, MINUTES).isAfter(currentToken.expires())) {
            transitionToRefreshing(steady, currentToken, new CompletableFuture<>());
        }
    }

    /**
     * attempt to transition to a new state, if we are in currentState
     * @param currentState the current state
     * @param toState the new state
     * @param onTransition invoked with the new state if transition succeeds
     * @return true if transition succeeds, false otherwise
     */
    private <T extends State> boolean transition(State currentState, T toState, Consumer<T> onTransition) {
        boolean transitioned = state.compareAndSet(currentState, toState);
        if (transitioned) {
            LOG.debug("transitioned from {} to {}", currentState, toState);
            onTransition.accept(toState);
        }
        return transitioned;
    }

    @VisibleForTesting
    State getState() {
        return state.get();
    }

    @Override
    public void close() {
        transitionToClosed();
    }

    private void transitionToRefreshing(State currentState, @Nullable BearerToken currentToken, CompletableFuture<BearerToken> tokenPromise) {
        transition(currentState, new State.Refreshing(currentToken, tokenPromise, clock), this::initiateRefresh);
    }

    private void transitionToInitialOnRefreshFailure(State.Refreshing currentState, Throwable t) {
        State.Initial toState = new State.Initial(new CompletableFuture<>());
        transition(currentState, toState, initial -> currentState.tokenPromise().completeExceptionally(t));
    }

    private void transitionToSteady(State currentState, BearerToken refreshing, Consumer<State.Steady> onTransition) {
        transition(currentState, new State.Steady(refreshing), onTransition);
    }

    private void transitionToClosed() {
        State current = state.get();
        boolean transitioned = transition(current, new State.Closed(),
                closed -> {
                    delegate.close();
                    if (current instanceof State.Refreshing refreshing) {
                        refreshing.promise.completeExceptionally(new IllegalStateException("token service closed"));
                    }
                    else if (current instanceof State.Initial initial) {
                        initial.tokenPromise().completeExceptionally(new IllegalStateException("token service closed"));
                    }
                });
        if (!transitioned) {
            // recurse until we transition to closed
            transitionToClosed();
        }
    }

    private void initiateRefresh(State.Refreshing refreshing) {
        try {
            CompletionStage<BearerToken> refresh = delegate.getBearerToken();
            refresh.whenComplete((BearerToken token, Throwable t) -> {
                if (t != null) {
                    onRefreshFailed(t, refreshing);
                }
                else {
                    onRefreshComplete(token, refreshing);
                }
            });
        }
        catch (Exception e) {
            // paranoid about delegate throwing in getBearerToken
            onRefreshFailed(e, refreshing);
        }
    }

    private void onRefreshFailed(Throwable t, State.Refreshing refreshing) {
        LOG.debug("refresh completed exceptionally", t);
        if (refreshing.current() != null) {
            // continue with existing token if it exists
            transitionToSteady(refreshing, refreshing.current(), steady -> refreshing.promise().completeExceptionally(t));
        }
        else {
            transitionToInitialOnRefreshFailure(refreshing, t);
        }
    }

    private void onRefreshComplete(BearerToken token, State.Refreshing refreshing) {
        LOG.debug("refresh completed successfully");
        transitionToSteady(refreshing, token, steady -> refreshing.promise().complete(token));
    }

}
