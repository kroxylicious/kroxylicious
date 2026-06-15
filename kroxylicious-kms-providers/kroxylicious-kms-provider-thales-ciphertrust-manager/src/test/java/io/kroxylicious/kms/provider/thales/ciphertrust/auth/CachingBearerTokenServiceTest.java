/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingBearerTokenServiceTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final Duration TOKEN_LIFETIME = Duration.ofMinutes(60);

    @Mock
    private BearerTokenService delegate;

    private final Clock clock = Clock.fixed(NOW, ZoneId.of("UTC"));

    @Test
    void shouldObtainInitialToken() {
        // Given
        BearerToken expectedToken = new BearerToken("jwt-token", NOW, NOW.plus(TOKEN_LIFETIME));
        when(delegate.getBearerToken()).thenReturn(CompletableFuture.completedFuture(expectedToken));

        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock);

        // When
        CompletableFuture<BearerToken> future = service.getBearerToken().toCompletableFuture();

        // Then
        assertThat(future).succeedsWithin(ofSeconds(1)).isEqualTo(expectedToken);
        verify(delegate, times(1)).getBearerToken();
    }

    @Test
    void shouldReturnCachedTokenWhenNotNearExpiry() {
        // Given
        BearerToken token = new BearerToken("jwt-token", NOW, NOW.plus(TOKEN_LIFETIME));
        when(delegate.getBearerToken()).thenReturn(CompletableFuture.completedFuture(token));

        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock);

        // When
        assertThat(service.getBearerToken().toCompletableFuture()).succeedsWithin(ofSeconds(1));
        CompletableFuture<BearerToken> cachedFuture = service.getBearerToken().toCompletableFuture();

        // Then
        assertThat(cachedFuture).succeedsWithin(ofSeconds(1)).isEqualTo(token);
        verify(delegate, times(1)).getBearerToken();
    }

    @Test
    void shouldRefreshTokenWhenNearExpiry() {
        // Given
        Instant firstCallTime = NOW;
        Instant secondCallTime = NOW.plus(Duration.ofMinutes(59).plusSeconds(30));

        BearerToken firstToken = new BearerToken("first-token", firstCallTime, firstCallTime.plus(TOKEN_LIFETIME));
        BearerToken secondToken = new BearerToken("second-token", secondCallTime, secondCallTime.plus(TOKEN_LIFETIME));

        when(delegate.getBearerToken())
                .thenReturn(CompletableFuture.completedFuture(firstToken))
                .thenReturn(CompletableFuture.completedFuture(secondToken));

        Clock clock1 = Clock.fixed(firstCallTime, ZoneId.of("UTC"));
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock1);

        // When
        CompletableFuture<BearerToken> future1 = service.getBearerToken().toCompletableFuture();

        Clock clock2 = Clock.fixed(secondCallTime, ZoneId.of("UTC"));
        service = new CachingBearerTokenService(delegate, new CachingBearerTokenService.State.Steady(firstToken), clock2);
        CompletableFuture<BearerToken> future2 = service.getBearerToken().toCompletableFuture();

        // Then
        assertThat(future1).succeedsWithin(ofSeconds(1)).isEqualTo(firstToken);
        assertThat(future2).succeedsWithin(ofSeconds(1)).isEqualTo(firstToken);
        verify(delegate, times(2)).getBearerToken();
    }

    @Test
    void shouldReturnCurrentTokenDuringRefreshIfNotExpired() {
        // Given
        BearerToken currentToken = new BearerToken("current", NOW, NOW.plus(TOKEN_LIFETIME));

        CompletableFuture<BearerToken> refreshFuture = new CompletableFuture<>();
        CachingBearerTokenService.State.Refreshing refreshingState = new CachingBearerTokenService.State.Refreshing(
                currentToken, refreshFuture, clock);
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, refreshingState, clock);

        // When
        CompletableFuture<BearerToken> future = service.getBearerToken().toCompletableFuture();

        // Then
        assertThat(future).succeedsWithin(ofSeconds(1)).isEqualTo(currentToken);
    }

    @Test
    void shouldWaitForRefreshIfCurrentTokenExpired() {
        // Given
        Instant expiredTime = NOW.minus(Duration.ofMinutes(10));
        BearerToken expiredToken = new BearerToken("expired", expiredTime, NOW.minus(Duration.ofMinutes(1)));

        CompletableFuture<BearerToken> promise = new CompletableFuture<>();
        CachingBearerTokenService.State.Refreshing refreshingState = new CachingBearerTokenService.State.Refreshing(
                expiredToken, promise, clock);
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, refreshingState, clock);

        // When
        CompletableFuture<BearerToken> future = service.getBearerToken().toCompletableFuture();

        // Then
        assertThat(future).isNotDone();
        assertThat(future).isSameAs(promise);

        BearerToken newToken = new BearerToken("new", NOW, NOW.plus(TOKEN_LIFETIME));
        promise.complete(newToken);

        assertThat(future).succeedsWithin(ofSeconds(1)).isEqualTo(newToken);
    }

    @Test
    void shouldHandleRefreshFailureWithExistingToken() throws Exception {
        // Given
        BearerToken currentToken = new BearerToken("current", NOW, NOW.plus(TOKEN_LIFETIME));

        CompletableFuture<BearerToken> delegateFuture = CompletableFuture.failedFuture(new RuntimeException("refresh failed"));
        when(delegate.getBearerToken()).thenReturn(delegateFuture);

        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock);
        BearerToken firstToken = new BearerToken("first", NOW, NOW.plus(TOKEN_LIFETIME));
        when(delegate.getBearerToken()).thenReturn(CompletableFuture.completedFuture(firstToken))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("refresh failed")));

        service = new CachingBearerTokenService(delegate, clock);
        assertThat(service.getBearerToken().toCompletableFuture()).succeedsWithin(ofSeconds(1));

        // When
        service = new CachingBearerTokenService(delegate, new CachingBearerTokenService.State.Steady(firstToken), clock);
        service.getBearerToken();

        // Then
        Thread.sleep(200);
        assertThat(service.getState()).isInstanceOf(CachingBearerTokenService.State.Steady.class);
        CachingBearerTokenService.State.Steady steadyState = (CachingBearerTokenService.State.Steady) service.getState();
        assertThat(steadyState.current()).isEqualTo(firstToken);
    }

    @Test
    void shouldHandleRefreshFailureWithoutExistingToken() throws Exception {
        // Given
        CompletableFuture<BearerToken> delegateFuture = CompletableFuture.failedFuture(new RuntimeException("refresh failed"));
        when(delegate.getBearerToken()).thenReturn(delegateFuture);

        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock);

        // When
        CompletableFuture<BearerToken> future = service.getBearerToken().toCompletableFuture();

        // Then
        Thread.sleep(200);
        assertThat(future).isCompletedExceptionally();
        assertThat(service.getState()).isInstanceOf(CachingBearerTokenService.State.Initial.class);
    }

    @Test
    void shouldTransitionToClosedAndFailPendingRequests() {
        // Given
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock);
        CompletableFuture<BearerToken> future = service.getBearerToken().toCompletableFuture();

        // When
        service.close();

        // Then
        assertThat(service.getState()).isInstanceOf(CachingBearerTokenService.State.Closed.class);
        assertThat(future)
                .failsWithin(ofSeconds(1))
                .withThrowableOfType(ExecutionException.class)
                .havingCause()
                .isInstanceOf(IllegalStateException.class)
                .withMessageContaining("token service closed");
        verify(delegate).close();
    }

    @Test
    void shouldFailRequestsWhenClosed() {
        // Given
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, clock);
        service.close();

        // When
        CompletableFuture<BearerToken> future = service.getBearerToken().toCompletableFuture();

        // Then
        assertThat(future)
                .failsWithin(ofSeconds(1))
                .withThrowableOfType(ExecutionException.class)
                .havingCause()
                .isInstanceOf(IllegalStateException.class)
                .withMessageContaining("service is closed");
    }

    @Test
    void shouldRejectNullDelegate() {
        assertThatThrownBy(() -> new CachingBearerTokenService(null, Clock.systemUTC()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("delegate cannot be null");
    }

    @Test
    void shouldRejectNullClock() {
        assertThatThrownBy(() -> new CachingBearerTokenService(delegate, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("clock cannot be null");
    }

    @Test
    void shouldRejectNullState() {
        assertThatThrownBy(() -> new CachingBearerTokenService(delegate, null, Clock.systemUTC()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("state cannot be null");
    }
}
