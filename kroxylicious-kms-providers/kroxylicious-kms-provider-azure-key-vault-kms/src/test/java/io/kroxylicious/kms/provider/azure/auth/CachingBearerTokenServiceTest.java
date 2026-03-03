/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.kms.provider.azure.auth.CachingBearerTokenService.State.Closed;
import io.kroxylicious.kms.provider.azure.auth.CachingBearerTokenService.State.Initial;
import io.kroxylicious.kms.provider.azure.auth.CachingBearerTokenService.State.Refreshing;
import io.kroxylicious.kms.provider.azure.auth.CachingBearerTokenService.State.Steady;
import io.kroxylicious.kms.service.KmsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CachingBearerTokenServiceTest {
    public static final BearerToken TOKEN = new BearerToken("token", Instant.MAX, Instant.MAX);
    @Mock
    private BearerTokenService delegate;

    @Test
    void initialGetBearerTokenTransitionsToRefresh() {
        // given
        when(delegate.getBearerToken()).thenReturn(new CompletableFuture<>());
        Initial initial = new Initial(new CompletableFuture<>());
        Clock clock = Clock.systemUTC();
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        // when
        service.getBearerToken();
        // then
        assertThat(initial.tokenPromise()).isNotDone();
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOfSatisfying(Refreshing.class, refreshing -> {
            assertThat(refreshing.tokenPromise()).isNotDone().isSameAs(initial.tokenPromise());
            assertThat(refreshing.promise()).isNotDone().isSameAs(initial.tokenPromise());
            assertThat(refreshing.current()).isNull();
            assertThat(refreshing.clock()).isSameAs(clock);
        });
    }

    @Test
    void initialGetBearerTokenSucceeds() {
        // given
        CompletableFuture<BearerToken> delegateFuture = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateFuture);
        CompletableFuture<BearerToken> initialTokenPromise = new CompletableFuture<>();
        Initial initial = new Initial(initialTokenPromise);
        Clock clock = Clock.systemUTC();
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        service.getBearerToken();
        CachingBearerTokenService.State state = service.getState();
        CompletableFuture<BearerToken> refreshTokenPromise = state.tokenPromise();
        // when
        delegateFuture.complete(TOKEN);
        // then
        assertThat(initialTokenPromise).succeedsWithin(Duration.ZERO).isEqualTo(TOKEN);
        assertThat(refreshTokenPromise).succeedsWithin(Duration.ZERO).isEqualTo(TOKEN);
        CachingBearerTokenService.State postSuccessState = service.getState();
        assertThat(postSuccessState).isInstanceOfSatisfying(Steady.class, s -> {
            assertThat(initialTokenPromise).succeedsWithin(Duration.ZERO).isEqualTo(TOKEN);
        });
    }

    @Test
    void initialGetBearerTokenFails() {
        // given
        CompletableFuture<BearerToken> delegateToken = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateToken);
        CompletableFuture<BearerToken> initialTokenPromise = new CompletableFuture<>();
        Initial initial = new Initial(initialTokenPromise);
        Clock clock = Clock.systemUTC();
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        service.getBearerToken();
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Refreshing.class);
        CompletableFuture<BearerToken> refreshTokenPromise = state.tokenPromise();
        verify(delegate).getBearerToken();
        // when
        KmsException fail = new KmsException("fail");
        delegateToken.completeExceptionally(fail);
        // then
        assertThat(initialTokenPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(fail);
        assertThat(refreshTokenPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(fail);
        CachingBearerTokenService.State postFailureState = service.getState();
        assertThat(postFailureState).isInstanceOf(Initial.class);
    }

    @Test
    void initialGetBearerTokenThrows() {
        // given
        KmsException fail = new KmsException("fail");
        when(delegate.getBearerToken()).thenThrow(fail);
        CompletableFuture<BearerToken> initialTokenPromise = new CompletableFuture<>();
        Initial initial = new Initial(initialTokenPromise);
        Clock clock = Clock.systemUTC();
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        // when
        service.getBearerToken();
        // then
        verify(delegate).getBearerToken();
        assertThat(initialTokenPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(fail);
        CachingBearerTokenService.State postFailureState = service.getState();
        assertThat(postFailureState).isInstanceOf(Initial.class);
    }

    @Test
    void getBearerTokenRefreshesIfLessThanOneMinuteToExpiry() {
        CompletableFuture<BearerToken> delegateToken = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateToken);
        Instant now = Instant.parse("2007-12-03T10:15:30.00Z");
        BearerToken token = new BearerToken("abcdef", Instant.MIN, now.plusSeconds(59));
        Steady initial = new Steady(token);
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        // when
        service.getBearerToken();
        // then
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOfSatisfying(Refreshing.class, refreshing -> {
            assertThat(refreshing.tokenPromise()).succeedsWithin(Duration.ZERO).isEqualTo(token);
            assertThat(refreshing.promise()).isNotDone();
            assertThat(refreshing.current()).isSameAs(token);
            assertThat(refreshing.clock()).isSameAs(clock);
        });
    }

    @Test
    void steadyRefreshFailureTransitionsBackToSteadyState() {
        CompletableFuture<BearerToken> delegateToken = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateToken);
        Instant now = Instant.parse("2007-12-03T10:15:30.00Z");
        BearerToken token = new BearerToken("abcdef", Instant.MIN, now.plusSeconds(59));
        Steady initial = new Steady(token);
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        service.getBearerToken();
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Refreshing.class);
        CompletableFuture<BearerToken> refreshPromise = ((Refreshing) state).promise();
        // when
        KmsException fail = new KmsException("fail");
        delegateToken.completeExceptionally(fail);
        // then
        CachingBearerTokenService.State postFailState = service.getState();
        assertThat(postFailState).isInstanceOfSatisfying(Steady.class, steady -> {
            assertThat(steady.tokenPromise()).succeedsWithin(Duration.ZERO).isEqualTo(token);
            assertThat(steady.current()).isSameAs(token);
        });
        assertThat(refreshPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(fail);
    }

    @Test
    void steadyRefreshSuccessTransitionsBackToSteadyState() {
        CompletableFuture<BearerToken> delegateToken = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateToken);
        Instant now = Instant.parse("2007-12-03T10:15:30.00Z");
        BearerToken token = new BearerToken("abcdef", Instant.MIN, now.plusSeconds(59));
        Steady initial = new Steady(token);
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        service.getBearerToken();
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Refreshing.class);
        CompletableFuture<BearerToken> refreshPromise = ((Refreshing) state).promise();
        // when
        delegateToken.complete(TOKEN);
        // then
        CachingBearerTokenService.State postFailState = service.getState();
        assertThat(postFailState).isInstanceOfSatisfying(Steady.class, steady -> {
            assertThat(steady.tokenPromise()).succeedsWithin(Duration.ZERO).isEqualTo(TOKEN);
            assertThat(steady.current()).isSameAs(TOKEN);
        });
        assertThat(refreshPromise).succeedsWithin(Duration.ZERO).isEqualTo(TOKEN);
    }

    @Test
    void closeBeforeRefreshFailure() {
        CompletableFuture<BearerToken> delegateToken = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateToken);
        Instant now = Instant.parse("2007-12-03T10:15:30.00Z");
        BearerToken token = new BearerToken("abcdef", Instant.MIN, now.plusSeconds(59));
        Steady initial = new Steady(token);
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        service.getBearerToken();
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Refreshing.class);
        CompletableFuture<BearerToken> refreshPromise = ((Refreshing) state).promise();
        // when
        service.close();
        KmsException fail = new KmsException("fail");
        delegateToken.completeExceptionally(fail);
        // then
        CachingBearerTokenService.State postFailState = service.getState();
        assertThat(postFailState).isInstanceOf(Closed.class);
        assertThat(refreshPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("token service closed");
        assertThat(service.getBearerToken()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("service is closed");
    }

    @Test
    void closeBeforeRefreshSuccess() {
        CompletableFuture<BearerToken> delegateToken = new CompletableFuture<>();
        when(delegate.getBearerToken()).thenReturn(delegateToken);
        Instant now = Instant.parse("2007-12-03T10:15:30.00Z");
        BearerToken token = new BearerToken("abcdef", Instant.MIN, now.plusSeconds(59));
        Steady initial = new Steady(token);
        Clock clock = Clock.fixed(now, ZoneId.of("UTC"));
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        service.getBearerToken();
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Refreshing.class);
        CompletableFuture<BearerToken> refreshPromise = ((Refreshing) state).promise();
        // when
        service.close();
        delegateToken.complete(TOKEN);
        // then
        CachingBearerTokenService.State postFailState = service.getState();
        assertThat(postFailState).isInstanceOf(Closed.class);
        assertThat(refreshPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("token service closed");
        assertThat(service.getBearerToken()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("service is closed");
    }

    @Test
    void initialToClosed() {
        CompletableFuture<BearerToken> initialPromise = new CompletableFuture<>();
        Initial initial = new Initial(initialPromise);
        Clock clock = Clock.systemUTC();
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, initial, clock);
        // when
        service.close();
        // then
        assertThat(initialPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class);
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Closed.class);
        assertThat(service.getBearerToken()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("service is closed");
    }

    @Test
    void refreshingToClosed() {
        CompletableFuture<BearerToken> refreshPromise = new CompletableFuture<>();
        Clock clock = Clock.systemUTC();
        Refreshing refreshing = new Refreshing(null, refreshPromise, clock);
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, refreshing, clock);
        // when
        service.close();
        // then
        assertThat(refreshPromise).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class);
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Closed.class);
        assertThat(service.getBearerToken()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("service is closed");
    }

    @Test
    void steadyToClosed() {
        Clock clock = Clock.systemUTC();
        Steady steady = new Steady(TOKEN);
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, steady, clock);
        // when
        service.close();
        // then
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Closed.class);
        assertThat(service.getBearerToken()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("service is closed");
    }

    @Test
    void closedToClosed() {
        Clock clock = Clock.systemUTC();
        Closed closed = new Closed();
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, closed, clock);
        // when
        service.close();
        // then
        CachingBearerTokenService.State state = service.getState();
        assertThat(state).isInstanceOf(Closed.class);
        assertThat(service.getBearerToken()).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause()
                .isInstanceOf(IllegalStateException.class).withMessage("service is closed");
    }

    @Test
    void validCachedTokenPreferredDuringRefresh() {
        Instant fixedInstant = Instant.parse("2007-12-03T10:15:30.00Z");
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        BearerToken token = new BearerToken("token", Instant.MIN, fixedInstant.plusNanos(1L));
        Refreshing refreshing = new Refreshing(token, new CompletableFuture<>(), clock);
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, refreshing, clock);
        assertThat(service.getBearerToken()).succeedsWithin(Duration.ZERO).isEqualTo(token);
    }

    @Test
    void promisePreferredIfCachedTokenExpiredDuringRefresh() {
        Instant fixedInstant = Instant.parse("2007-12-03T10:15:30.00Z");
        Clock clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
        Instant expiryInPast = fixedInstant.minusNanos(1L);
        BearerToken token = new BearerToken("token", Instant.MIN, expiryInPast);
        CompletableFuture<BearerToken> promise = new CompletableFuture<>();
        Refreshing refreshing = new Refreshing(token, promise, clock);
        CachingBearerTokenService service = new CachingBearerTokenService(delegate, refreshing, clock);
        assertThat(service.getBearerToken()).isSameAs(promise);
    }

    public static Stream<Arguments> stateToStringsDoNotContainCredentials() {
        BearerToken mytoken = new BearerToken("mytoken", Instant.MAX, Instant.MAX);
        return Stream.of(Arguments.argumentSet("initial", new Initial(CompletableFuture.completedFuture(mytoken))),
                Arguments.argumentSet("initial", new Refreshing(mytoken, CompletableFuture.completedFuture(mytoken), Clock.systemUTC())),
                Arguments.argumentSet("initial", new Steady(mytoken)),
                Arguments.argumentSet("closed", new Closed()));
    }

    @MethodSource
    @ParameterizedTest
    void stateToStringsDoNotContainCredentials(CachingBearerTokenService.State state) {
        assertThat(state.toString()).doesNotContain("mytoken");
    }

}