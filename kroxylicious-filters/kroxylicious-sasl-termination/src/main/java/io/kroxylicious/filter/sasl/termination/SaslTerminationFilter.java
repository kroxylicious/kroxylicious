/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
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

import io.kroxylicious.filter.sasl.termination.mechanism.AuthenticationResult;
import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler;
import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * SASL termination filter.
 * <p>
 * Terminates SASL authentication at the proxy, authenticating clients against
 * pluggable credential stores. Enforces a security barrier preventing unauthenticated
 * requests. Supports reauthentication (KIP-368).
 * </p>
 */
public class SaslTerminationFilter implements RequestFilter, ApiVersionsResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTerminationFilter.class);

    static final String AUTH_DURATION_METRIC = "kroxylicious_filter_sasl_termination_auth_duration_seconds";
    static final String SESSION_EXPIRED_METRIC = "kroxylicious_filter_sasl_termination_session_expired_total";
    private static final String MECHANISM_TAG = "mechanism";

    private static final Set<Short> FILTERED_API_KEYS = Set.of(
            ApiKeys.CREATE_DELEGATION_TOKEN.id,
            ApiKeys.RENEW_DELEGATION_TOKEN.id,
            ApiKeys.EXPIRE_DELEGATION_TOKEN.id,
            ApiKeys.DESCRIBE_DELEGATION_TOKEN.id,
            ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.id);

    private final SaslTermination.SaslTerminationContext context;
    private final Clock clock;
    private final long maxTimeBeforeReauthMs;
    private final SaslSubjectBuilder subjectBuilder;
    private State state;
    private long authStartNanos;
    @Nullable
    private String lastAuthenticatedMechanism;

    public SaslTerminationFilter(SaslTermination.SaslTerminationContext context) {
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
                case API_VERSIONS, SASL_HANDSHAKE, SASL_AUTHENTICATE, CREATE_DELEGATION_TOKEN, RENEW_DELEGATION_TOKEN, EXPIRE_DELEGATION_TOKEN, DESCRIBE_DELEGATION_TOKEN, ALTER_USER_SCRAM_CREDENTIALS, DESCRIBE_USER_SCRAM_CREDENTIALS -> true;
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
            case CREATE_DELEGATION_TOKEN, RENEW_DELEGATION_TOKEN, EXPIRE_DELEGATION_TOKEN, DESCRIBE_DELEGATION_TOKEN -> rejectUnsupportedApi(header, request, apiKey,
                    "Delegation tokens are not supported when SASL is terminated at the proxy", filterContext);
            case ALTER_USER_SCRAM_CREDENTIALS -> rejectUnsupportedApi(header, request, apiKey,
                    "SCRAM credentials cannot be modified via the Kafka protocol when SASL is terminated at the proxy", filterContext);
            case DESCRIBE_USER_SCRAM_CREDENTIALS -> onDescribeUserScramCredentials((DescribeUserScramCredentialsRequestData) request, filterContext);
            case SASL_HANDSHAKE -> {
                if (isUnsupportedApiVersion(ApiKeys.SASL_HANDSHAKE, apiVersion, filterContext)) {
                    yield rejectUnsupportedVersionAndClose(header, request, apiKey, apiVersion, filterContext);
                }
                yield onSaslHandshakeRequest((SaslHandshakeRequestData) request, filterContext);
            }
            case SASL_AUTHENTICATE -> {
                if (isUnsupportedApiVersion(ApiKeys.SASL_AUTHENTICATE, apiVersion, filterContext)) {
                    yield rejectUnsupportedVersionAndClose(header, request, apiKey, apiVersion, filterContext);
                }
                yield onSaslAuthenticateRequest((SaslAuthenticateRequestData) request, filterContext);
            }
            default -> handleDefaultRequest(header, request, filterContext);
        };
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header,
                                                                       ApiVersionsResponseData response, FilterContext context) {
        response.apiKeys().removeIf(apiVersion1 -> FILTERED_API_KEYS.contains(apiVersion1.apiKey()));
        return context.forwardResponse(header, response);
    }

    private CompletionStage<RequestFilterResult> onDescribeUserScramCredentials(
                                                                                DescribeUserScramCredentialsRequestData request,
                                                                                FilterContext filterContext) {

        Map<Byte, ScramCredentialStore> stores = context.scramCredentialStores();
        if (stores.isEmpty()) {
            var response = new DescribeUserScramCredentialsResponseData()
                    .setErrorCode(Errors.NONE.code());
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(response)
                    .completed();
        }

        var userNames = request.users();
        if (userNames == null || userNames.isEmpty()) {
            var response = new DescribeUserScramCredentialsResponseData()
                    .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
                    .setErrorMessage("Listing all users is not supported; specify user names");
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(response)
                    .completed();
        }

        List<CompletionStage<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult>> resultFutures = new ArrayList<>();
        for (var userName : userNames) {
            resultFutures.add(describeUser(userName.name(), stores));
        }

        CompletionStage<List<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult>> allResults = CompletableFuture
                .completedFuture(new ArrayList<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult>());
        for (var resultFuture : resultFutures) {
            allResults = allResults.thenCombine(resultFuture, (list, result) -> {
                list.add(result);
                return list;
            });
        }

        return allResults.thenCompose(results -> {
            var response = new DescribeUserScramCredentialsResponseData()
                    .setErrorCode(Errors.NONE.code());
            response.results().addAll(results);
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(response)
                    .completed();
        });
    }

    private CompletionStage<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult> describeUser(
                                                                                                                      String username,
                                                                                                                      Map<Byte, ScramCredentialStore> stores) {
        var result = new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                .setUser(username);

        List<CompletionStage<Void>> lookups = new ArrayList<>();
        for (var entry : stores.entrySet()) {
            byte mechanismType = entry.getKey();
            ScramCredentialStore store = entry.getValue();
            lookups.add(store.lookupCredential(username).thenAccept(credential -> {
                if (credential != null) {
                    result.credentialInfos().add(
                            new DescribeUserScramCredentialsResponseData.CredentialInfo()
                                    .setMechanism(mechanismType)
                                    .setIterations(credential.iterations()));
                }
            }));
        }

        CompletionStage<Void> allDone = CompletableFuture.completedFuture(null);
        for (var lookup : lookups) {
            allDone = allDone.thenCombine(lookup, (a, b) -> null);
        }

        return allDone
                .thenApply(v -> {
                    if (result.credentialInfos().isEmpty()) {
                        result.setErrorCode(Errors.RESOURCE_NOT_FOUND.code());
                        result.setErrorMessage("Attempt to describe a user credential that does not exist: " + username);
                    }
                    else {
                        result.setErrorCode(Errors.NONE.code());
                    }
                    return result;
                })
                .exceptionally(throwable -> {
                    LOGGER.atWarn()
                            .addKeyValue("username", username)
                            .addKeyValue("error", throwable.getMessage())
                            .log("Failed to describe SCRAM credentials for user");
                    result.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                    result.setErrorMessage("Credential lookup failed");
                    return result;
                });
    }

    private CompletionStage<RequestFilterResult> onSaslHandshakeRequest(
                                                                        SaslHandshakeRequestData request,
                                                                        FilterContext filterContext) {

        if (!(state instanceof State.RequiringHandshake) && !(state instanceof State.Authenticated)) {
            LOGGER.atWarn()
                    .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                    .addKeyValue("state", state)
                    .log("Received SASL handshake in unexpected state");
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setMechanisms(List.of()))
                    .completed();
        }

        String mechanism = request.mechanism();
        Errors errorCode;
        List<String> supportedMechanisms;

        if (context.handlerFactories().containsKey(mechanism)) {
            MechanismHandlerFactory factory = context.handlerFactories().get(mechanism);
            MechanismHandler handler = factory.createHandler();

            authStartNanos = System.nanoTime();
            if (state instanceof State.RequiringHandshake handshake) {
                state = handshake.nextState(handler);
            }
            else if (state instanceof State.Authenticated authenticated) {
                LOGGER.atDebug()
                        .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                        .addKeyValue("mechanism", mechanism)
                        .log("Reauthentication initiated");
                state = authenticated.nextStateReauthenticate(handler);
            }

            errorCode = Errors.NONE;
            supportedMechanisms = List.of();
        }
        else {
            LOGGER.atDebug()
                    .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                    .addKeyValue("mechanism", mechanism)
                    .log("Unsupported mechanism");
            errorCode = Errors.UNSUPPORTED_SASL_MECHANISM;
            supportedMechanisms = List.copyOf(context.handlerFactories().keySet());
        }

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(errorCode.code())
                        .setMechanisms(supportedMechanisms))
                .completed();
    }

    private CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(
                                                                           SaslAuthenticateRequestData request,
                                                                           FilterContext filterContext) {

        if (!(state instanceof State.RequiringAuthenticate authenticating)) {
            LOGGER.atWarn()
                    .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                    .addKeyValue("state", state)
                    .log("Received SASL authenticate in unexpected state");
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslAuthenticateResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setErrorMessage("Authentication not in progress")
                            .setAuthBytes(new byte[0]))
                    .completed();
        }

        MechanismHandler handler = authenticating.mechanismHandler();

        int maxAuthBytes = handler.maxAuthBytes();
        if (request.authBytes().length > maxAuthBytes) {
            LOGGER.atWarn()
                    .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                    .addKeyValue("mechanism", handler.mechanismName())
                    .addKeyValue("payloadSize", request.authBytes().length)
                    .addKeyValue("maxPayloadSize", maxAuthBytes)
                    .log("Rejecting oversized SASL authenticate payload");
            return handleAuthenticationFailure(
                    "Authentication payload exceeds maximum size",
                    handler,
                    filterContext);
        }

        return handler.handleAuthenticate(request.authBytes())
                .thenCompose(result -> processAuthenticationResult(result, handler, filterContext))
                .exceptionallyCompose(throwable -> {
                    LOGGER.atError()
                            .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                            .setCause(throwable)
                            .log("Authentication error");
                    return handleAuthenticationFailure(
                            "Internal error: " + throwable.getMessage(),
                            handler,
                            filterContext);
                });
    }

    private CompletionStage<RequestFilterResult> processAuthenticationResult(
                                                                             AuthenticationResult result,
                                                                             MechanismHandler handler,
                                                                             FilterContext filterContext) {

        return switch (result.outcome()) {
            case CHALLENGE -> {
                if (state instanceof State.RequiringAuthenticate authenticating) {
                    state = authenticating.nextStateChallenge();
                }

                yield filterContext.requestFilterResultBuilder()
                        .shortCircuitResponse(new SaslAuthenticateResponseData()
                                .setErrorCode(Errors.NONE.code())
                                .setAuthBytes(result.responseBytes()))
                        .completed();
            }

            case SUCCESS -> {
                String authorizationId = result.authorizationId();
                String mechanism = handler.mechanismName();
                lastAuthenticatedMechanism = mechanism;
                recordAuthDuration(mechanism);
                LOGGER.atDebug()
                        .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                        .addKeyValue("authorizationId", authorizationId)
                        .log("Authentication successful");

                long sessionLifetimeMs = computeSessionLifetimeMs(result.sessionLifetimeMs());
                @Nullable
                Instant sessionExpiry = sessionLifetimeMs > 0 ? clock.instant().plusMillis(sessionLifetimeMs) : null;

                if (state instanceof State.RequiringAuthenticate authenticating) {
                    state = authenticating.nextStateSuccess(authorizationId, sessionExpiry);
                }

                handler.dispose();

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

                yield subjectBuilder.buildSaslSubject(subjectContext)
                        .thenCompose(subject -> {
                            filterContext.clientSaslAuthenticationSuccess(mechanism, subject);
                            return filterContext.requestFilterResultBuilder()
                                    .shortCircuitResponse(new SaslAuthenticateResponseData()
                                            .setErrorCode(Errors.NONE.code())
                                            .setAuthBytes(result.responseBytes())
                                            .setSessionLifetimeMs(sessionLifetimeMs))
                                    .completed();
                        });
            }

            case FAILURE -> handleAuthenticationFailure(result.errorMessage(), handler, filterContext);
        };
    }

    /**
     * Compute the effective session lifetime as the minimum of the configured
     * maximum and the handler-reported lifetime (KIP-368).
     */
    private long computeSessionLifetimeMs(long handlerLifetimeMs) {
        if (maxTimeBeforeReauthMs > 0 && handlerLifetimeMs > 0) {
            return Math.min(maxTimeBeforeReauthMs, handlerLifetimeMs);
        }
        return Math.max(maxTimeBeforeReauthMs, handlerLifetimeMs);
    }

    private CompletionStage<RequestFilterResult> handleAuthenticationFailure(
                                                                             String errorMessage,
                                                                             MechanismHandler handler,
                                                                             FilterContext filterContext) {
        recordAuthDuration(handler.mechanismName());
        LOGGER.atDebug()
                .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                .addKeyValue("error", errorMessage)
                .log("Authentication failed");

        if (state instanceof State.RequiringAuthenticate authenticating) {
            state = authenticating.nextStateFailure(errorMessage);
        }

        handler.dispose();

        filterContext.clientSaslAuthenticationFailure(handler.mechanismName(), null,
                new IllegalStateException(errorMessage));

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                        .setErrorMessage(errorMessage)
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
                if (lastAuthenticatedMechanism != null) {
                    Counter.builder(SESSION_EXPIRED_METRIC)
                            .tag(MECHANISM_TAG, lastAuthenticatedMechanism)
                            .register(Metrics.globalRegistry)
                            .increment();
                }
                LOGGER.atDebug()
                        .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
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
                    .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
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
                .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("reason", reason)
                .log("Rejecting unsupported API request");
        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception(reason))
                .completed();
    }

    private void recordAuthDuration(String mechanism) {
        long durationNanos = System.nanoTime() - authStartNanos;
        Timer.builder(AUTH_DURATION_METRIC)
                .tag(MECHANISM_TAG, mechanism)
                .register(Metrics.globalRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    private static boolean isUnsupportedApiVersion(ApiKeys apiKey, short apiVersion, FilterContext filterContext) {
        return apiVersion < apiKey.oldestVersion() || apiVersion > apiKey.latestVersion();
    }

    private CompletionStage<RequestFilterResult> rejectUnsupportedVersionAndClose(
                                                                                  RequestHeaderData header,
                                                                                  ApiMessage request,
                                                                                  ApiKeys apiKey,
                                                                                  short apiVersion,
                                                                                  FilterContext filterContext) {
        LOGGER.atWarn()
                .addKeyValue("channelDescriptor", filterContext.channelDescriptor())
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .log("Rejecting SASL request with unsupported API version");
        return filterContext.requestFilterResultBuilder()
                .errorResponse(header, request, Errors.UNSUPPORTED_VERSION.exception())
                .withCloseConnection()
                .completed();
    }
}
