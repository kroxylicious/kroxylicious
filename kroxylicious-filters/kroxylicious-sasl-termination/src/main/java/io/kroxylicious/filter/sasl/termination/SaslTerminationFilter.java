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
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.sasl.termination.mechanism.AuthenticationResult;
import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler;
import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * SASL termination filter.
 * <p>
 * Terminates SASL authentication at the proxy, authenticating clients against
 * pluggable credential stores. Enforces a security barrier preventing unauthenticated
 * requests. Supports reauthentication (KIP-368).
 * </p>
 */
public class SaslTerminationFilter implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTerminationFilter.class);

    private final SaslTermination.SaslTerminationContext context;
    private final Clock clock;
    private final long maxTimeBeforeReauthMs;
    private State state;

    public SaslTerminationFilter(SaslTermination.SaslTerminationContext context) {
        this.context = context;
        this.clock = context.clock();
        Duration maxReauth = context.maxTimeBeforeReauth();
        this.maxTimeBeforeReauthMs = maxReauth != null ? maxReauth.toMillis() : 0;
        this.state = State.start();
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
            case SASL_HANDSHAKE -> onSaslHandshakeRequest((SaslHandshakeRequestData) request, filterContext);
            case SASL_AUTHENTICATE -> onSaslAuthenticateRequest((SaslAuthenticateRequestData) request, filterContext);
            default -> handleDefaultRequest(header, request, filterContext);
        };
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

                Subject subject = new Subject(new User(authorizationId));
                filterContext.clientSaslAuthenticationSuccess(handler.mechanismName(), subject);

                yield filterContext.requestFilterResultBuilder()
                        .shortCircuitResponse(new SaslAuthenticateResponseData()
                                .setErrorCode(Errors.NONE.code())
                                .setAuthBytes(result.responseBytes())
                                .setSessionLifetimeMs(sessionLifetimeMs))
                        .completed();
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
}
