/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>SASL termination filter.</p>
 * <p>
 * Terminates SASL authentication at the proxy, authenticating clients against
 * pluggable credential stores. Enforces a security barrier preventing unauthenticated
 * requests. Supports reauthentication (KIP-368).
 * </p>
 * <p>The Kafka protocol uses two APIs for SASL authentication: </p>
 * <ol>
 * <li>the client chooses its preferred SASL mechanism using {@code SaslHandshake}</li>
 * <li>the client then makes one or more {@code SaslAuthenticate} requests</li>
 * </ol>
 * <p>This class is responsible for understanding the Kafka-specific parts.
 * It drives a state machine (see {@link State}) that models the expected sequence of Kafka requests.
 * During authentication, the mechanism-specific transition logic is provided by a
 * {@link MechanismStateMachine}.</p>
 */
public class SaslTerminationFilter implements RequestFilter, ApiVersionsResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTerminationFilter.class);

    private static final String LOG_KEY_SESSION_ID = "sessionId";
    private static final String LOG_KEY_MECHANISM = "mechanism";
    private static final String LOG_KEY_STATE = "state";
    private static final String LOG_KEY_ERROR = "error";

    static final String AUTH_DURATION_METRIC = "kroxylicious_filter_sasl_termination_auth_duration_seconds";
    static final String SESSION_EXPIRED_METRIC = "kroxylicious_filter_sasl_termination_session_expired_total";
    private static final String MECHANISM_TAG = "mechanism";

    private static final Set<Short> FILTERED_API_KEYS = Set.of(
            ApiKeys.CREATE_DELEGATION_TOKEN.id,
            ApiKeys.RENEW_DELEGATION_TOKEN.id,
            ApiKeys.EXPIRE_DELEGATION_TOKEN.id,
            ApiKeys.DESCRIBE_DELEGATION_TOKEN.id);

    private final ScheduledExecutorService executorService;
    private final SaslTermination.SaslTerminationContext context;
    private final Clock clock;
    private final long maxTimeBeforeReauthMs;
    private final SaslSubjectBuilder subjectBuilder;
    private State state;

    /**
     * Constructs the filter.
     *
     * @param executorService the executor for scheduling delayed responses
     * @param context the SASL termination context
     */
    SaslTerminationFilter(ScheduledExecutorService executorService, SaslTermination.SaslTerminationContext context) {
        this.executorService = executorService;
        this.context = context;
        this.clock = context.clock();
        Duration maxReauth = context.maxTimeBeforeReauth();
        this.maxTimeBeforeReauthMs = maxReauth != null ? maxReauth.toMillis() : 0;
        this.subjectBuilder = context.subjectBuilder();
        this.state = State.start();
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        if (state instanceof State.Authenticated authenticated && authenticated.sessionExpiry() == null) {
            return switch (apiKey) {
                case API_VERSIONS, SASL_HANDSHAKE, SASL_AUTHENTICATE, CREATE_DELEGATION_TOKEN, RENEW_DELEGATION_TOKEN, EXPIRE_DELEGATION_TOKEN, DESCRIBE_DELEGATION_TOKEN -> true;
                default -> false;
            };
        }
        return true;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(
                                                          ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext filterContext) {

        return switch (apiKey) {
            case API_VERSIONS -> filterContext.forwardRequest(header, request);
            case SASL_HANDSHAKE -> {
                if (isUnsupportedApiVersion(ApiKeys.SASL_HANDSHAKE, apiVersion)) {
                    yield rejectUnsupportedVersionAndClose(header, request, apiKey, apiVersion, filterContext);
                }
                yield onSaslHandshakeRequest((SaslHandshakeRequestData) request, filterContext);
            }
            case SASL_AUTHENTICATE -> {
                if (isUnsupportedApiVersion(ApiKeys.SASL_AUTHENTICATE, apiVersion)) {
                    yield rejectUnsupportedVersionAndClose(header, request, apiKey, apiVersion, filterContext);
                }
                yield onSaslAuthenticateRequest((SaslAuthenticateRequestData) request, filterContext);
            }
            case CREATE_DELEGATION_TOKEN, RENEW_DELEGATION_TOKEN, EXPIRE_DELEGATION_TOKEN, DESCRIBE_DELEGATION_TOKEN -> rejectUnsupportedApi(header, request, apiKey,
                    "Delegation tokens are not supported when SASL is terminated at the proxy", filterContext);
            default -> handleDefaultRequest(header, request, filterContext);
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header,
                                                                       ApiVersionsResponseData response, FilterContext context) {
        response.apiKeys().removeIf(apiVersion1 -> FILTERED_API_KEYS.contains(apiVersion1.apiKey()));
        return context.forwardResponse(header, response);
    }

    private CompletionStage<RequestFilterResult> onSaslHandshakeRequest(
                                                                        SaslHandshakeRequestData request,
                                                                        FilterContext filterContext) {

        if (!(state instanceof State.RequiringHandshake) && !(state instanceof State.Authenticated)) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_STATE, state)
                    .log("Received SASL handshake in unexpected state");
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setMechanisms(List.of()))
                    .completed();
        }

        String mechanism = request.mechanism();

        MechanismStateMachine stateMachine = createStateMachine(mechanism);
        if (stateMachine != null) {
            long authStartNanos = System.nanoTime();
            if (state instanceof State.RequiringHandshake handshake) {
                state = handshake.nextState(stateMachine, authStartNanos);
            }
            else if (state instanceof State.Authenticated authenticated) {
                LOGGER.atDebug()
                        .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                        .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                        .log("Reauthentication initiated");
                state = authenticated.nextStateReauthenticate(stateMachine, authStartNanos);
            }

            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setMechanisms(List.of()))
                    .completed();
        }
        else {
            LOGGER.atDebug()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                    .log("Unsupported mechanism");
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData()
                            .setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code())
                            .setMechanisms(List.copyOf(context.supportedMechanisms())))
                    .withCloseConnection()
                    .completed();
        }
    }

    @Nullable
    private MechanismStateMachine createStateMachine(String mechanism) {
        if (!context.supportedMechanisms().contains(mechanism)) {
            return null;
        }
        return switch (mechanism) {
            case "OAUTHBEARER" -> new OauthBearerStateMachine(Objects.requireNonNull(context.oauthCallbackHandler()),
                    context.clock());
            default -> throw new IllegalStateException("No state machine for configured mechanism: " + mechanism);
        };
    }

    private CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(
                                                                           SaslAuthenticateRequestData request,
                                                                           FilterContext filterContext) {

        if (!(state instanceof State.RequiringAuthenticate authenticating)) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_STATE, state)
                    .log("Received SASL authenticate in unexpected state");
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslAuthenticateResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setErrorMessage("Authentication not in progress")
                            .setAuthBytes(new byte[0]))
                    .completed();
        }

        MechanismStateMachine stateMachine = authenticating.mechanismStateMachine();

        int maxAuthBytes = stateMachine.maxAuthBytes();
        if (request.authBytes().length > maxAuthBytes) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_MECHANISM, stateMachine.mechanismName())
                    .addKeyValue("payloadSize", request.authBytes().length)
                    .addKeyValue("maxPayloadSize", maxAuthBytes)
                    .log("Rejecting oversized SASL authenticate payload");
            return handleAuthenticationFailure(
                    stateMachine, filterContext, new InvalidRequestException("Authentication payload exceeds maximum size"));
        }

        Instant authRoundStart = clock.instant();
        return stateMachine.evaluateRound(request.authBytes())
                .thenCompose(result -> applyFixedAuthDelay(result, authRoundStart, stateMachine.mechanismName()))
                .thenCompose(result -> processRoundResult(result, stateMachine, filterContext))
                .exceptionallyCompose(throwable -> {
                    LOGGER.atError()
                            .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                            .setCause(throwable)
                            .log("Authentication error");
                    Exception exception = throwable instanceof Exception e ? e : new RuntimeException(throwable);
                    return handleAuthenticationFailure(stateMachine, filterContext, exception);
                });
    }

    private CompletionStage<RequestFilterResult> processRoundResult(
                                                                    RoundResult result,
                                                                    MechanismStateMachine stateMachine,
                                                                    FilterContext filterContext) {

        return switch (result) {
            case RoundResult.Challenge challenge -> handleChallenge(filterContext, challenge);
            case RoundResult.Success success -> handleSuccess(stateMachine, filterContext, success);
            case RoundResult.Failure failure -> handleAuthenticationFailure(stateMachine, filterContext, failure.exception());
        };
    }

    @NonNull
    private CompletionStage<RequestFilterResult> handleSuccess(MechanismStateMachine stateMachine, FilterContext filterContext,
                                                               RoundResult.Success success) {
        String authorizationId = success.authorizationId();
        String mechanism = stateMachine.mechanismName();
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_MECHANISM, stateMachine.mechanismName())
                .addKeyValue("authorizationId", authorizationId)
                .log("Authentication successful");

        long sessionLifetimeMs = computeSessionLifetimeMs(success.sessionLifetimeMs());
        @Nullable
        Instant sessionExpiry = sessionLifetimeMs > 0 ? clock.instant().plusMillis(sessionLifetimeMs) : null;

        if (state instanceof State.RequiringAuthenticate authenticating) {
            recordAuthDuration(mechanism, authenticating.authStartNanos());
            state = authenticating.nextStateSuccess(authorizationId, mechanism, sessionExpiry);
        }

        stateMachine.dispose();

        SaslSubjectBuilder.Context subjectContext = new SaslSubjectBuilder.Context() {
            @Override
            public Optional<ClientTlsContext> clientTlsContext() {
                return filterContext.clientTlsContext();
            }

            @Override
            public ClientSaslContext clientSaslContext() {
                return new ClientSaslContext() {
                    @Override
                    public String mechanismName() {
                        return mechanism;
                    }

                    @Override
                    public String authorizationId() {
                        return authorizationId;
                    }
                };
            }
        };

        return subjectBuilder.buildSaslSubject(subjectContext)
                .thenCompose(subject -> {
                    filterContext.clientSaslAuthenticationSuccess(mechanism, subject);
                    return filterContext.requestFilterResultBuilder()
                            .shortCircuitResponse(new SaslAuthenticateResponseData()
                                    .setErrorCode(Errors.NONE.code())
                                    .setAuthBytes(success.responseBytes())
                                    .setSessionLifetimeMs(sessionLifetimeMs))
                            .completed();
                });
    }

    @NonNull
    private CompletionStage<RequestFilterResult> handleChallenge(FilterContext filterContext, RoundResult.Challenge challenge) {
        if (state instanceof State.RequiringAuthenticate authenticating) {
            state = authenticating.nextStateChallenge();
        }

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setAuthBytes(challenge.responseBytes()))
                .completed();
    }

    /**
     * Compute the effective session lifetime as the minimum of the configured
     * maximum and the mechanism-reported lifetime (KIP-368).
     */
    @VisibleForTesting
    long computeSessionLifetimeMs(long handlerLifetimeMs) {
        if (maxTimeBeforeReauthMs > 0 && handlerLifetimeMs > 0) {
            return Math.min(maxTimeBeforeReauthMs, handlerLifetimeMs);
        }
        return Math.max(maxTimeBeforeReauthMs, handlerLifetimeMs);
    }

    private CompletionStage<RequestFilterResult> handleAuthenticationFailure(
                                                                             MechanismStateMachine stateMachine, FilterContext filterContext, Exception exception) {
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_ERROR, exception.getMessage())
                .log("Authentication failed");

        if (state instanceof State.RequiringAuthenticate authenticating) {
            recordAuthDuration(stateMachine.mechanismName(), authenticating.authStartNanos());
            state = authenticating.nextStateFailure(exception.getMessage());
        }

        stateMachine.dispose();

        filterContext.clientSaslAuthenticationFailure(stateMachine.mechanismName(), null, exception);

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                        .setErrorMessage("Authentication failed")
                        .setAuthBytes(new byte[0]))
                .withCloseConnection()
                .completed();
    }

    private CompletionStage<RequestFilterResult> handleDefaultRequest(
                                                                      RequestHeaderData header,
                                                                      ApiMessage request,
                                                                      FilterContext filterContext) {

        if (state instanceof State.Authenticated authenticated) {
            Instant expiry = authenticated.sessionExpiry();
            if (expiry != null && clock.instant().isAfter(expiry)) {
                Counter.builder(SESSION_EXPIRED_METRIC)
                        .tag(MECHANISM_TAG, authenticated.mechanismName())
                        .register(Metrics.globalRegistry)
                        .increment();
                LOGGER.atDebug()
                        .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                        .addKeyValue("sessionExpiry", expiry)
                        .log("Session expired, rejecting request");
                return filterContext.requestFilterResultBuilder()
                        .errorResponse(header, request, Errors.SASL_AUTHENTICATION_FAILED.exception())
                        .withCloseConnection()
                        .completed();
            }
            return filterContext.forwardRequest(header, request);
        }
        else {
            LOGGER.atDebug()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue("requestType", request.getClass().getSimpleName())
                    .log("Rejecting unauthenticated request");

            return filterContext.requestFilterResultBuilder()
                    .errorResponse(header, request, Errors.SASL_AUTHENTICATION_FAILED.exception())
                    .withCloseConnection()
                    .completed();
        }
    }

    private static CompletionStage<RequestFilterResult> rejectUnsupportedApi(
                                                                             RequestHeaderData header,
                                                                             ApiMessage request,
                                                                             ApiKeys apiKey,
                                                                             String reason,
                                                                             FilterContext filterContext) {
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("reason", reason)
                .log("Rejecting unsupported API request");
        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception(reason))
                .completed();
    }

    private CompletionStage<RoundResult> applyFixedAuthDelay(RoundResult result, Instant start, String mechanismName) {
        Duration fixedAuthDelay = context.fixedAuthDelay();
        if (fixedAuthDelay.isZero()) {
            return CompletableFuture.completedFuture(result);
        }
        Duration elapsed = Duration.between(start, clock.instant());
        if (elapsed.compareTo(fixedAuthDelay) > 0) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_MECHANISM, mechanismName)
                    .addKeyValue("elapsed", elapsed)
                    .addKeyValue("fixedAuthDelay", fixedAuthDelay)
                    .log("Authentication took longer than fixedAuthDelay, consider increasing fixedAuthDelay");
        }
        return delayUntil(start.plus(fixedAuthDelay), result);
    }

    private CompletionStage<RoundResult> delayUntil(Instant deadline, RoundResult result) {
        long remainingMs = Duration.between(clock.instant(), deadline).toMillis();
        if (remainingMs <= 0) {
            return CompletableFuture.completedFuture(result);
        }
        CompletableFuture<RoundResult> future = new CompletableFuture<>();
        executorService.schedule(() -> future.complete(result), remainingMs, TimeUnit.MILLISECONDS);
        return future;
    }

    private static void recordAuthDuration(String mechanism, long authStartNanos) {
        long durationNanos = System.nanoTime() - authStartNanos;
        Timer.builder(AUTH_DURATION_METRIC)
                .tag(MECHANISM_TAG, mechanism)
                .register(Metrics.globalRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    private static boolean isUnsupportedApiVersion(ApiKeys apiKey, short apiVersion) {
        return apiVersion < apiKey.oldestVersion() || apiVersion > apiKey.latestVersion();
    }

    private CompletionStage<RequestFilterResult> rejectUnsupportedVersionAndClose(
                                                                                  RequestHeaderData header,
                                                                                  ApiMessage request,
                                                                                  ApiKeys apiKey,
                                                                                  short apiVersion,
                                                                                  FilterContext filterContext) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .log("Rejecting SASL request with unsupported API version");
        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception())
                .withCloseConnection()
                .completed();
    }
}
