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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

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
class SaslTerminationFilter implements RequestFilter, ApiVersionsResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTerminationFilter.class);

    private static final String LOG_KEY_SESSION_ID = "sessionId";
    private static final String LOG_KEY_MECHANISM = "mechanism";
    private static final String LOG_KEY_STATE = "state";
    private static final String LOG_KEY_ERROR = "error";
    private static final String LOG_KEY_REAUTHENTICATION = "reauthentication";
    private static final String LOG_KEY_REASON = "reason";
    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";

    static final String AUTH_DURATION_METRIC = "kroxylicious_filter_sasl_termination_auth_duration_seconds";
    static final String SESSION_EXPIRED_METRIC = "kroxylicious_filter_sasl_termination_session_expired_total";
    private static final String MECHANISM_TAG = "mechanism";
    static final String VIRTUAL_CLUSTER_TAG = "virtual_cluster";

    private static final Set<Short> FILTERED_API_KEYS = Set.of(
            ApiKeys.CREATE_DELEGATION_TOKEN.id,
            ApiKeys.RENEW_DELEGATION_TOKEN.id,
            ApiKeys.EXPIRE_DELEGATION_TOKEN.id,
            ApiKeys.DESCRIBE_DELEGATION_TOKEN.id,
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.id,
            ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS.id);

    private final FilterDispatchExecutor executorService;
    private final SaslTermination.SaslTerminationContext context;
    private final Clock clock;
    private final long maxTimeBeforeReauthMs;
    private final SaslSubjectBuilder subjectBuilder;
    private State state;

    @VisibleForTesting
    void forceState(State state) {
        this.state = state;
    }

    /**
     * Constructs the filter.
     *
     * @param executorService the filter dispatch executor, used for scheduling delayed responses and ensuring thread safety
     * @param context the SASL termination context
     */
    SaslTerminationFilter(FilterDispatchExecutor executorService, SaslTermination.SaslTerminationContext context) {
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
                case API_VERSIONS, SASL_HANDSHAKE, SASL_AUTHENTICATE -> true;
                default -> FILTERED_API_KEYS.contains(apiKey.id);
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
            case SASL_HANDSHAKE -> onSaslHandshakeRequest(apiKey, apiVersion, header, (SaslHandshakeRequestData) request, filterContext);
            case SASL_AUTHENTICATE -> onSaslAuthenticateRequest(apiKey, apiVersion, header, (SaslAuthenticateRequestData) request, filterContext);
            default -> onAnyOtherRequest(apiKey, header, request, filterContext);
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header,
                                                                       ApiVersionsResponseData response, FilterContext context) {
        response.apiKeys().removeIf(apiVersion1 -> FILTERED_API_KEYS.contains(apiVersion1.apiKey()));
        return context.forwardResponse(header, response);
    }

    private CompletionStage<RequestFilterResult> onSaslHandshakeRequest(
                                                                        ApiKeys apiKey,
                                                                        short apiVersion,
                                                                        RequestHeaderData header,
                                                                        SaslHandshakeRequestData request,
                                                                        FilterContext filterContext) {

        if (isUnsupportedApiVersion(ApiKeys.SASL_HANDSHAKE, apiVersion)) {
            return rejectUnsupportedVersionAndClose(filterContext, apiKey, apiVersion, header, request);
        }

        if (!(state instanceof State.RequiringHandshake) && !(state instanceof State.Authenticated)) {
            return rejectHandshakeNotExpected(filterContext);
        }

        String mechanism = request.mechanism();

        if (state instanceof State.Authenticated authenticated && !mechanism.equals(authenticated.mechanismName())) {
            return rejectHandshakeReauthMechanismChange(filterContext, authenticated, mechanism);
        }

        MechanismStateMachine stateMachine = createStateMachine(mechanism);
        if (stateMachine == null) {
            return rejectHandshakeUnsupportedMechanism(filterContext, mechanism);
        }
        else {
            return acceptHandshake(filterContext, stateMachine, mechanism);
        }
    }

    private CompletionStage<RequestFilterResult> acceptHandshake(FilterContext filterContext,
                                                                 MechanismStateMachine stateMachine,
                                                                 String mechanism) {
        if (state instanceof State.RequiringHandshake handshake) {
            state = handshake.nextState(stateMachine);
        }
        else if (state instanceof State.Authenticated authenticated) {
            LOGGER.atDebug()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                    .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                    .log("Reauthentication initiated");
            state = authenticated.nextStateReauthenticate(stateMachine);
        }

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setMechanisms(List.of()))
                .completed();
    }

    private CompletionStage<RequestFilterResult> rejectHandshakeUnsupportedMechanism(FilterContext filterContext,
                                                                                     String mechanism) {
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                .log("Unsupported mechanism");
        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code())
                        .setMechanisms(List.copyOf(context.supportedMechanisms())))
                .withCloseConnection()
                .completed();
    }

    private static CompletionStage<RequestFilterResult> rejectHandshakeReauthMechanismChange(FilterContext filterContext,
                                                                                             State.Authenticated authenticated,
                                                                                             String mechanism) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                .addKeyValue("previousMechanism", authenticated.mechanismName())
                .log("Reauthentication rejected: mechanism change not permitted");
        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                        .setMechanisms(List.of()))
                .withCloseConnection()
                .completed();
    }

    private CompletionStage<RequestFilterResult> rejectHandshakeNotExpected(FilterContext filterContext) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_STATE, state)
                .log("Received SASL handshake in unexpected state");
        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                        .setMechanisms(List.of()))
                .completed();
    }

    @Nullable
    private MechanismStateMachine createStateMachine(String mechanism) {
        if (!context.supportedMechanisms().contains(mechanism)) {
            return null;
        }
        return switch (mechanism) {
            case OauthBearerMechanismConfig.MECHANISM_NAME -> new OauthBearerStateMachine(Objects.requireNonNull(context.oauthCallbackHandler()),
                    context.oauthMaxAuthBytes());
            case ScramMechanismConfig.MECHANISM_NAME_SCRAM_SHA_256 -> new ScramStateMachine(ScramMechanism.SCRAM_SHA_256,
                    context.scramCredentialStores().get(ScramMechanism.SCRAM_SHA_256),
                    context.scramPhantomIterations().get(ScramMechanism.SCRAM_SHA_256),
                    executorService);
            case ScramMechanismConfig.MECHANISM_NAME_SCRAM_SHA_512 -> new ScramStateMachine(ScramMechanism.SCRAM_SHA_512,
                    context.scramCredentialStores().get(ScramMechanism.SCRAM_SHA_512),
                    context.scramPhantomIterations().get(ScramMechanism.SCRAM_SHA_512),
                    executorService);
            default -> throw new IllegalStateException("No state machine for configured mechanism: " + mechanism);
        };
    }

    private CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(
                                                                           ApiKeys apiKey,
                                                                           short apiVersion,
                                                                           RequestHeaderData header,
                                                                           SaslAuthenticateRequestData request,
                                                                           FilterContext filterContext) {

        if (isUnsupportedApiVersion(ApiKeys.SASL_AUTHENTICATE, apiVersion)) {
            return rejectUnsupportedVersionAndClose(filterContext, apiKey, apiVersion, header, request);
        }

        if (!(state instanceof State.RequiringAuthenticate authenticating)) {
            return rejectAuthenticateNotExpected(filterContext);
        }

        MechanismStateMachine stateMachine = authenticating.mechanismStateMachine();

        int maxAuthBytes = stateMachine.maxAuthBytes();
        if (request.authBytes().length > maxAuthBytes) {
            return rejectAuthenticateBytesTooLarge(filterContext, request, stateMachine, maxAuthBytes);
        }

        Instant authRoundStart = clock.instant();
        long roundStartNanos = System.nanoTime();
        return stateMachine.evaluateRound(request.authBytes())
                .whenComplete((result, ex) -> authenticating.addRoundDuration(System.nanoTime() - roundStartNanos))
                .handle(RoundOutcome::new)
                .thenCompose(outcome -> applyFixedAuthDelay(filterContext, outcome, authRoundStart, stateMachine.mechanismName()))
                .thenCompose(outcome -> {
                    if (outcome.exception() != null) {
                        return rejectAuthenticateInternalError(filterContext, stateMachine,
                                "stateMachine",
                                outcome.exception());
                    }
                    return switch (outcome.result()) {
                        case RoundResult.Challenge challenge -> acceptAuthenticateContinue(filterContext, challenge);
                        case RoundResult.Success success -> processMechanismSuccess(filterContext, stateMachine, success);
                        case RoundResult.Failure failure -> rejectAuthenticateMechanismFailed(filterContext, stateMachine, failure.exception());
                    };
                });
    }

    private CompletionStage<RequestFilterResult> rejectAuthenticateBytesTooLarge(FilterContext filterContext,
                                                                                 SaslAuthenticateRequestData request,
                                                                                 MechanismStateMachine stateMachine,
                                                                                 int maxAuthBytes) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_MECHANISM, stateMachine.mechanismName())
                .addKeyValue("payloadSize", request.authBytes().length)
                .addKeyValue("maxPayloadSize", maxAuthBytes)
                .log("Rejecting oversized SASL authenticate payload");
        return rejectAuthenticateAndClose(
                filterContext, stateMachine,
                new InvalidRequestException("Authentication payload exceeds maximum size"));
    }

    private CompletionStage<RequestFilterResult> rejectAuthenticateNotExpected(FilterContext filterContext) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_STATE, state)
                .log("Received SASL authenticate in unexpected state");
        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                        .setErrorMessage("Authentication not in progress")
                        .setAuthBytes(new byte[0]))
                .completed();
    }

    private CompletionStage<RequestFilterResult> processMechanismSuccess(FilterContext filterContext,
                                                                         MechanismStateMachine stateMachine,
                                                                         RoundResult.Success success) {

        if (!(state instanceof State.RequiringAuthenticate authenticating)) {
            // this should be impossible
            throw new IllegalStateException("handleSuccess called in unexpected state: " + state);
        }
        String authorizationId = success.authorizationId();
        String mechanism = stateMachine.mechanismName();
        boolean reauthentication = authenticating.previousAuthorizationId() != null;
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                .addKeyValue(LOG_KEY_REAUTHENTICATION, reauthentication)
                .addKeyValue("authorizationId", authorizationId)
                .log("Credential validation successful");

        Instant sessionExpiry = computeSessionExpiry(success.sessionExpiry());

        String previousAuthorizationId = authenticating.previousAuthorizationId();
        if (previousAuthorizationId != null
                && !previousAuthorizationId.equals(authorizationId)) {
            return rejectAuthenticateIdChanged(filterContext, stateMachine, previousAuthorizationId, authorizationId);
        }

        stateMachine.dispose();

        return subjectBuilder.buildSaslSubject(new SubjectContext(filterContext, mechanism, authorizationId))
                .thenCompose(subject -> completeSubjectBuild(filterContext, stateMachine,
                        success,
                        subject,
                        mechanism,
                        reauthentication,
                        sessionExpiry))
                .exceptionallyCompose(throwable -> rejectAuthenticateInternalError(filterContext, stateMachine,
                        "subjectBuilder",
                        throwable));
    }

    private CompletionStage<RequestFilterResult> rejectAuthenticateIdChanged(FilterContext filterContext,
                                                                             MechanismStateMachine stateMachine,
                                                                             String previousAuthorizationId,
                                                                             String authorizationId) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_REAUTHENTICATION, true)
                .addKeyValue("previousAuthorizationId", previousAuthorizationId)
                .addKeyValue("newAuthorizationId", authorizationId)
                .log("Reauthentication rejected: authorization ID changed");
        return rejectAuthenticateAndClose(filterContext, stateMachine,
                new SaslAuthenticationException("Reauthentication failed: authorization identity changed"));
    }

    private CompletionStage<RequestFilterResult> completeSubjectBuild(FilterContext filterContext,
                                                                      MechanismStateMachine stateMachine,
                                                                      RoundResult.Success success,
                                                                      Subject subject,
                                                                      String mechanism,
                                                                      boolean reauthentication,
                                                                      @Nullable Instant sessionExpiry) {
        if (sessionExpiry != null && !clock.instant().isBefore(sessionExpiry)) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                    .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                    .log("Token expired during authentication");
            return rejectAuthenticateAndClose(filterContext, stateMachine,
                    new SaslAuthenticationException("Token expired during authentication"));
        }

        return acceptAuthenticateDone(filterContext, success, subject, mechanism, reauthentication, sessionExpiry);
    }

    private CompletionStage<RequestFilterResult> acceptAuthenticateDone(FilterContext filterContext,
                                                                        RoundResult.Success success,
                                                                        Subject subject,
                                                                        String mechanism,
                                                                        boolean reauthentication,
                                                                        @Nullable Instant sessionExpiry) {
        if (state instanceof State.RequiringAuthenticate authenticating) {
            recordAuthDuration(mechanism, filterContext.getVirtualClusterName(),
                    Duration.ofNanos(authenticating.accumulatedAuthWorkNanos()));
            state = authenticating.nextStateSuccess(success.authorizationId(), mechanism, sessionExpiry);
        }

        String authorizationId = success.authorizationId();
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_MECHANISM, mechanism)
                .addKeyValue("authorizationId", authorizationId)
                .addKeyValue(LOG_KEY_REAUTHENTICATION, reauthentication)
                .log("Authentication successful");

        filterContext.clientSaslAuthenticationSuccess(mechanism, subject);

        long sessionLifetimeMs = sessionExpiry != null ? Duration.between(clock.instant(), sessionExpiry).toMillis() : 0;
        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setAuthBytes(success.responseBytes())
                        .setSessionLifetimeMs(sessionLifetimeMs))
                .completed();
    }

    private CompletionStage<RequestFilterResult> rejectAuthenticateInternalError(FilterContext filterContext,
                                                                                 MechanismStateMachine stateMachine,
                                                                                 String origin,
                                                                                 Throwable throwable) {
        LOGGER.atError()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_MECHANISM, stateMachine.mechanismName())
                .setCause(throwable)
                .addKeyValue("origin", origin)
                .log("Authentication error");
        Exception exception = throwable instanceof Exception e ? e : new RuntimeException(throwable);
        return rejectAuthenticateAndClose(filterContext, stateMachine,
                exception);
    }

    private CompletionStage<RequestFilterResult> acceptAuthenticateContinue(FilterContext filterContext,
                                                                            RoundResult.Challenge challenge) {
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
     * Compute the effective session expiry as the earlier of the configured
     * maximum reauth time and the mechanism-reported expiry (KIP-368).
     */
    @VisibleForTesting
    @Nullable
    Instant computeSessionExpiry(@Nullable Instant mechanismExpiry) {
        Instant maxReauthExpiry = maxTimeBeforeReauthMs > 0 ? clock.instant().plusMillis(maxTimeBeforeReauthMs) : null;
        if (maxReauthExpiry != null && mechanismExpiry != null) {
            return maxReauthExpiry.isBefore(mechanismExpiry) ? maxReauthExpiry : mechanismExpiry;
        }
        return maxReauthExpiry != null ? maxReauthExpiry : mechanismExpiry;
    }

    private CompletionStage<RequestFilterResult> rejectAuthenticateMechanismFailed(
                                                                                   FilterContext filterContext, MechanismStateMachine stateMachine,
                                                                                   Exception exception) {
        boolean reauthentication = state instanceof State.RequiringAuthenticate req
                && req.previousAuthorizationId() != null;
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue(LOG_KEY_ERROR, exception.getMessage())
                .addKeyValue(LOG_KEY_REAUTHENTICATION, reauthentication)
                .log("Authentication failed");
        return rejectAuthenticateAndClose(filterContext, stateMachine, exception);
    }

    private CompletionStage<RequestFilterResult> rejectAuthenticateAndClose(
                                                                            FilterContext filterContext,
                                                                            MechanismStateMachine stateMachine,
                                                                            Exception exception) {

        if (state instanceof State.RequiringAuthenticate authenticating) {
            String mechanism = stateMachine.mechanismName();
            String virtualClusterName = filterContext.getVirtualClusterName();
            Duration authDuration = Duration.ofNanos(authenticating.accumulatedAuthWorkNanos());
            recordAuthDuration(mechanism, virtualClusterName, authDuration);
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

    private CompletionStage<RequestFilterResult> onAnyOtherRequest(
                                                                   ApiKeys apiKey,
                                                                   RequestHeaderData header,
                                                                   ApiMessage request,
                                                                   FilterContext filterContext) {
        if (state instanceof State.Authenticated authenticated) {
            Instant expiry = authenticated.sessionExpiry();
            if (expiry == null || !clock.instant().isAfter(expiry)) {
                if (FILTERED_API_KEYS.contains(apiKey.id)) {
                    return rejectUnsupportedApi(filterContext, header,
                            request,
                            apiKey,
                            apiKey + " is not supported when SASL is terminated at the proxy");
                }
                else {
                    return filterContext.forwardRequest(header, request);
                }
            }
            Counter.builder(SESSION_EXPIRED_METRIC)
                    .description("Number of sessions that expired without the client reauthenticating in time.")
                    .tag(MECHANISM_TAG, authenticated.mechanismName())
                    .tag(VIRTUAL_CLUSTER_TAG, filterContext.getVirtualClusterName())
                    .register(Metrics.globalRegistry)
                    .increment();
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                    .addKeyValue(LOG_KEY_REASON, "SASL session expired")
                    .addKeyValue("sessionExpiry", expiry)
                    .addKeyValue("requestType", request.getClass().getSimpleName())
                    .log("Rejecting request from unauthenticated client");
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                    .addKeyValue(LOG_KEY_REASON, "Client is not authenticated")
                    .addKeyValue(LOG_KEY_STATE, state)
                    .addKeyValue("requestType", request.getClass().getSimpleName())
                    .log("Rejecting request from unauthenticated client");
        }

        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.SASL_AUTHENTICATION_FAILED)
                .withCloseConnection()
                .completed();
    }

    private static CompletionStage<RequestFilterResult> rejectUnsupportedApi(
                                                                             FilterContext filterContext,
                                                                             RequestHeaderData header,
                                                                             ApiMessage request,
                                                                             ApiKeys apiKey,
                                                                             String reason) {
        LOGGER.atDebug()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue(LOG_KEY_REASON, reason)
                .log("Rejecting unsupported API request");
        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION, reason)
                .completed();
    }

    private CompletionStage<RoundOutcome> applyFixedAuthDelay(FilterContext filterContext,
                                                              RoundOutcome outcome,
                                                              Instant start,
                                                              String mechanismName) {
        Duration fixedAuthDelay = context.fixedAuthDelay();
        if (fixedAuthDelay.isZero()) {
            return CompletableFuture.completedFuture(outcome);
        }
        Duration elapsed = Duration.between(start, clock.instant());
        if (elapsed.compareTo(fixedAuthDelay) > 0) {
            LOGGER.atWarn()
                    .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                    .addKeyValue(LOG_KEY_MECHANISM, mechanismName)
                    .addKeyValue("elapsed", elapsed)
                    .addKeyValue("fixedAuthDelay", fixedAuthDelay)
                    .log("Authentication took longer than fixedAuthDelay, consider increasing fixedAuthDelay");
        }
        return delayUntil(start.plus(fixedAuthDelay), outcome);
    }

    @SuppressWarnings("FutureReturnValueIgnored") // the ScheduledFuture is not needed; completion is observed via the returned CompletableFuture
    private CompletionStage<RoundOutcome> delayUntil(Instant deadline, RoundOutcome outcome) {
        long remainingMs = Duration.between(clock.instant(), deadline).toMillis();
        if (remainingMs <= 0) {
            return CompletableFuture.completedFuture(outcome);
        }
        CompletableFuture<RoundOutcome> future = new CompletableFuture<>();
        executorService.schedule(() -> future.complete(outcome), remainingMs, TimeUnit.MILLISECONDS);
        return future;
    }

    record RoundOutcome(@Nullable RoundResult result, @Nullable Throwable exception) {}

    private static void recordAuthDuration(String mechanism,
                                           String virtualClusterName,
                                           Duration authDuration) {
        Timer.builder(AUTH_DURATION_METRIC)
                .description(
                        "Authentication latency, exclusive of the configured fixed timing delay. Measures the real work: credential store lookup, token validation, SCRAM rounds.")
                .tag(MECHANISM_TAG, mechanism)
                .tag(VIRTUAL_CLUSTER_TAG, virtualClusterName)
                .register(Metrics.globalRegistry)
                .record(authDuration);
    }

    private static boolean isUnsupportedApiVersion(ApiKeys apiKey, short apiVersion) {
        return apiVersion < apiKey.oldestVersion() || apiVersion > apiKey.latestVersion();
    }

    private CompletionStage<RequestFilterResult> rejectUnsupportedVersionAndClose(
                                                                                  FilterContext filterContext,
                                                                                  ApiKeys apiKey,
                                                                                  short apiVersion,
                                                                                  RequestHeaderData header,
                                                                                  ApiMessage request) {
        LOGGER.atWarn()
                .addKeyValue(LOG_KEY_SESSION_ID, filterContext.sessionId())
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, filterContext.getVirtualClusterName())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .log("Rejecting SASL request with unsupported API version");
        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION)
                .withCloseConnection()
                .completed();
    }

    private static class SubjectContext implements SaslSubjectBuilder.Context {
        private final FilterContext filterContext;
        private final String mechanism;
        private final String authorizationId;

        SubjectContext(FilterContext filterContext, String mechanism, String authorizationId) {
            this.filterContext = filterContext;
            this.mechanism = mechanism;
            this.authorizationId = authorizationId;
        }

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
    }
}
