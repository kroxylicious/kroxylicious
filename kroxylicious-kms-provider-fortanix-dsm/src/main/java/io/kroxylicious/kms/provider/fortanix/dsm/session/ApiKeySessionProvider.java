/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.fortanix.dsm.FortanixDsmKms;
import io.kroxylicious.kms.provider.fortanix.dsm.config.ApiKeySessionProviderConfig;
import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provider that obtains a {@link Session} using a Fortanix Api Key.
 * <p>
 * The provider will keep returning the same session until the session reaches
 * a configured factor of the session's lifespan.  At which point, a preemptive
 * background refresh of the session will be performed.  Until the refresh is complete
 * the caller will continue to receive the existing session.  Once the refresh is complete
 * subsequent calls will see the updated session.
 * </p>
 * <p>
 * If an error occurs whilst retrieving the credential, the next call will cause the
 * provider to try again.  A progressive backoff is applied to retry attempts.
 * </p>
 *
 * TODO - consider using /sys/v1/session/refresh (or /sys/v1/session/reauth) endpoint to refresh existing session,
 *        rather than starting over from scratch
 * TODO - allow caller to indicate that the session has gone stale (i.e. before expiry, say in the case
 *        where the server side is restarted)
 */
public class ApiKeySessionProvider implements SessionProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiKeySessionProvider.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<SessionAuthResponse> SESSION_AUTH_RESPONSE = new TypeReference<>() {
    };
    private static final Duration HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HTTP_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final double DEFAULT_CREDENTIALS_LIFETIME_FACTOR = 0.80;
    public static final String SESSION_AUTH_ENDPOINT = "/sys/v1/session/auth";

    private final Clock systemClock;
    private final AtomicReference<CompletableFuture<Session>> current = new AtomicReference<>();

    private final AtomicLong tokenRefreshErrorCount = new AtomicLong();
    private final Config config;
    private final HttpClient client;

    private final ScheduledExecutorService executorService;
    @SuppressWarnings({ "java:S2245", "java:S2119" }) // Random used for backoff jitter, it does not need to be securely random.
    private final ExponentialBackoff backoff = new ExponentialBackoff(500, 2, 60000, new Random().nextDouble());
    private final Double lifetimeFactor;

    /**
     * Creates a session provider that uses an Api Key to authenticate.
     *
     * @param config config.
     */
    public ApiKeySessionProvider(@NonNull Config config) {
        this(config, Clock.systemUTC());
    }

    @VisibleForTesting
    ApiKeySessionProvider(@NonNull Config config, @NonNull Clock systemClock) {
        Objects.requireNonNull(config);
        Objects.requireNonNull(systemClock);
        this.config = config;
        this.systemClock = systemClock;
        this.lifetimeFactor = Optional.ofNullable(config.apiKeyConfig().sessionLifetimeFactor()).orElse(DEFAULT_CREDENTIALS_LIFETIME_FACTOR);
        this.executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            var thread = new Thread(r, ApiKeySessionProviderConfig.class.getName() + "thread");
            thread.setDaemon(true);
            return thread;
        });
        this.client = createClient();
    }

    private HttpClient createClient() {
        var builder = HttpClient.newBuilder();
        return builder
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(HTTP_CONNECT_TIMEOUT)
                .build();
    }

    @NonNull
    @Override
    public CompletionStage<Session> getSession() {
        var newCredFuture = new CompletableFuture<Session>();
        var witness = current.compareAndExchange(null, newCredFuture);
        if (witness == null) {
            // there's no current credential, let's create one
            executorService.execute(() -> refreshCredential(newCredFuture));
            return newCredFuture.minimalCompletionStage();
        }
        else if (isExpired(witness) || witness.isCompletedExceptionally()) {
            // current credential is expired, or it has been completed exceptionally.
            // throw it away and generate a new one.
            // we don't normally expect to follow the expired path as the preemptive refresh ought to have
            // caused its refresh before its expiration.
            current.compareAndSet(witness, null);
            return getSession();
        }

        return witness.minimalCompletionStage();
    }

    private void scheduleCredentialRefresh(long delay) {
        LOGGER.debug("Scheduling refresh of Fortanix session in {}ms", delay);

        var refreshedCredFuture = new CompletableFuture<Session>();
        executorService.schedule(() -> {
            refreshCredential(refreshedCredFuture);
            refreshedCredFuture.thenApply(sc -> {
                var previous = current.getAndSet(refreshedCredFuture);
                // the previous future have been already complete, but for safety, complete it anyway.
                Optional.ofNullable(previous).ifPresent(f -> f.complete(sc));
                return null;
            });
        }, delay, TimeUnit.MILLISECONDS);
    }

    private boolean isExpired(CompletableFuture<Session> witness) {
        if (witness.isDone() && !witness.isCompletedExceptionally()) {
            try {
                return Optional.ofNullable(witness.getNow(null))
                        .map(Session::expiration)
                        .map(exp -> systemClock.instant().isAfter(exp))
                        .orElse(false);
            }
            catch (CancellationException | CompletionException e) {
                return false;
            }
        }
        return false;
    }

    private void refreshCredential(CompletableFuture<Session> future) {
        var sessionRequest = createSessionAuthRequest();
        client.sendAsync(sessionRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(ApiKeySessionProvider::checkResponseStatus)
                .thenApply(HttpResponse::body)
                .thenApply(this::toSecurityAuthResponse)
                .thenApply(this::toSession)
                .whenComplete((credentials, t) -> propagateResultToFuture(credentials, t, future));
    }

    private void propagateResultToFuture(Session credentials, Throwable t, CompletableFuture<Session> target) {
        final long refreshDelay;
        if (t != null) {
            LOGGER.warn("Refresh of session failed", t);
            tokenRefreshErrorCount.incrementAndGet();
            target.completeExceptionally(t);

            refreshDelay = backoff.backoff(tokenRefreshErrorCount.get());
        }
        else {
            var expiration = credentials.expiration();
            LOGGER.debug("Obtained Fortanix DSM session, expiry: {}", expiration);
            tokenRefreshErrorCount.set(0);
            target.complete(credentials);

            refreshDelay = (long) Math.max(0, this.lifetimeFactor * (expiration.toEpochMilli() - systemClock.instant().toEpochMilli()));
        }
        scheduleCredentialRefresh(refreshDelay);
    }

    private HttpRequest createSessionAuthRequest() {
        var providedPassword = this.config.apiKeyConfig().apiKey().getProvidedPassword();
        return HttpRequest.newBuilder()
                .uri(config.endpointUrl().resolve(SESSION_AUTH_ENDPOINT))
                .header(FortanixDsmKms.AUTHORIZATION_HEADER, "Basic " + providedPassword)
                .timeout(HTTP_REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    private Session toSession(SessionAuthResponse sessionAuthResponse) {
        var authzHeader = sessionAuthResponse.tokenType() + " " + sessionAuthResponse.accessToken();
        var expiration = systemClock.instant().plusSeconds(sessionAuthResponse.expiresIn());
        return new Session() {
            @NonNull
            @Override
            public String authorizationHeader() {
                return authzHeader;
            }

            @NonNull
            @Override
            public Instant expiration() {
                return expiration;
            }
        };
    }

    private SessionAuthResponse toSecurityAuthResponse(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, SESSION_AUTH_RESPONSE);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to unmarshal '%s' as a SessionAuthResponse.".formatted(bodyToString(bytes)), e);
        }
    }

    @NonNull
    private static <O> HttpResponse<O> checkResponseStatus(@NonNull HttpResponse<O> response) {
        var statusCode = response.statusCode();
        var httpSuccess = statusCode >= 200 && statusCode < 300;
        if (!httpSuccess) {
            var uri = response.request().uri();
            var body = bodyToString(response.body());
            throw new KmsException("Operation failed, request uri: %s, HTTP status code %d, response: %s".formatted(uri, statusCode, body));
        }
        return response;
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }

    private static <B> String bodyToString(B body) {
        return body instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(body);
    }

}
