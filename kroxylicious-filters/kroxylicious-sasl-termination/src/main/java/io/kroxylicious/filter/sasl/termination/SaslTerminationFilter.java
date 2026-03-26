/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * SASL termination filter.
 * <p>
 * Terminates SASL authentication at the proxy, authenticating clients against
 * pluggable credential stores. Enforces a security barrier preventing unauthenticated
 * requests.
 * </p>
 *
 * <h2>State Machine</h2>
 * <p>
 * The filter maintains per-connection state using the {@link State} sealed interface:
 * </p>
 * <pre>
 * START ──→ RequiringHandshake
 *                    │
 *                    ↓
 *           RequiringAuthenticate ←──╮
 *                    │                │
 *                    ├─ (multi-round) ─╯
 *                    │
 *                    ├──→ Authenticated (terminal, success)
 *                    │
 *                    └──→ Failed (terminal, failure)
 * </pre>
 *
 * <h2>Security Barrier</h2>
 * <p>
 * Only {@code API_VERSIONS}, {@code SASL_HANDSHAKE}, and {@code SASL_AUTHENTICATE}
 * are allowed before authentication. All other requests return
 * {@code SASL_AUTHENTICATION_FAILED} and close the connection.
 * </p>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * Filter instances are per-connection and accessed only from the connection's
 * event loop thread. Not thread-safe.
 * </p>
 */
public class SaslTerminationFilter implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTerminationFilter.class);

    private final SaslTermination.SaslTerminationContext context;
    private State state;

    public SaslTerminationFilter(@NonNull SaslTermination.SaslTerminationContext context) {
        this.context = context;
        this.state = State.start();
    }

    @Override
    @NonNull
    public CompletionStage<RequestFilterResult> onRequest(
                                                          @NonNull ApiKeys apiKey,
                                                          short apiVersion,
                                                          @NonNull RequestHeaderData header,
                                                          @NonNull ApiMessage request,
                                                          @NonNull FilterContext filterContext) {

        return switch (apiKey) {
            case API_VERSIONS -> filterContext.forwardRequest(header, request);
            case SASL_HANDSHAKE -> onSaslHandshakeRequest((SaslHandshakeRequestData) request, filterContext);
            case SASL_AUTHENTICATE -> onSaslAuthenticateRequest((SaslAuthenticateRequestData) request, filterContext);
            default -> handleDefaultRequest(header, request, filterContext);
        };
    }

    /**
     * Handle SASL handshake request.
     * <p>
     * Validates the requested mechanism and transitions to RequiringAuthenticate state.
     * </p>
     */
    @NonNull
    private CompletionStage<RequestFilterResult> onSaslHandshakeRequest(
                                                                        @NonNull SaslHandshakeRequestData request,
                                                                        @NonNull FilterContext filterContext) {

        if (!(state instanceof State.RequiringHandshake)) {
            LOGGER.warn("{}: Received SASL handshake in state {}", filterContext.channelDescriptor(), state);
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setMechanisms(List.of()))
                    .completed();
        }

        String mechanism = request.mechanism();
        Errors errorCode;
        List<String> supportedMechanisms;

        // Check if mechanism is supported
        if (context.credentialStores().containsKey(mechanism) &&
                context.handlerFactories().containsKey(mechanism)) {

            // Create handler for this mechanism
            MechanismHandlerFactory factory = context.handlerFactories().get(mechanism);
            MechanismHandler handler = factory.createHandler();

            // Transition to RequiringAuthenticate state
            state = ((State.RequiringHandshake) state).nextState(handler);

            errorCode = Errors.NONE;
            supportedMechanisms = List.of();
        }
        else {
            LOGGER.debug("{}: Unsupported mechanism: {}", filterContext.channelDescriptor(), mechanism);
            errorCode = Errors.UNSUPPORTED_SASL_MECHANISM;
            supportedMechanisms = List.copyOf(context.credentialStores().keySet());
        }

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(errorCode.code())
                        .setMechanisms(supportedMechanisms))
                .completed();
    }

    /**
     * Handle SASL authenticate request.
     * <p>
     * Processes authentication bytes through the mechanism handler and transitions
     * state based on the result (CHALLENGE/SUCCESS/FAILURE).
     * </p>
     */
    @NonNull
    private CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(
                                                                           @NonNull SaslAuthenticateRequestData request,
                                                                           @NonNull FilterContext filterContext) {

        if (!(state instanceof State.RequiringAuthenticate authenticating)) {
            LOGGER.warn("{}: Received SASL authenticate in state {}", filterContext.channelDescriptor(), state);
            return filterContext.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslAuthenticateResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setErrorMessage("Authentication not in progress")
                            .setAuthBytes(new byte[0]))
                    .completed();
        }

        MechanismHandler handler = authenticating.mechanismHandler();
        String mechanismName = handler.mechanismName();

        // Get credential store for this mechanism
        ScramCredentialStore credentialStore = context.credentialStores().get(mechanismName);

        // Process authentication asynchronously
        return handler.handleAuthenticate(request.authBytes(), credentialStore)
                .thenCompose(result -> processAuthenticationResult(result, handler, filterContext))
                .exceptionally(throwable -> {
                    LOGGER.error("{}: Authentication error", filterContext.channelDescriptor(), throwable);
                    return handleAuthenticationFailure(
                            "Internal error: " + throwable.getMessage(),
                            handler,
                            filterContext);
                });
    }

    /**
     * Process authentication result and transition state.
     */
    @NonNull
    private CompletionStage<RequestFilterResult> processAuthenticationResult(
                                                                             @NonNull AuthenticationResult result,
                                                                             @NonNull MechanismHandler handler,
                                                                             @NonNull FilterContext filterContext) {

        return switch (result.outcome()) {
            case CHALLENGE -> {
                // Multi-round authentication continues
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
                LOGGER.debug("{}: Authentication successful, authorizationId={}",
                        filterContext.channelDescriptor(), authorizationId);

                // Transition to Authenticated state
                if (state instanceof State.RequiringAuthenticate authenticating) {
                    state = authenticating.nextStateSuccess(authorizationId);
                }

                // Dispose handler
                handler.dispose();

                // Notify proxy of successful authentication
                Subject subject = new Subject(new User(authorizationId));
                filterContext.clientSaslAuthenticationSuccess(handler.mechanismName(), subject);

                yield filterContext.requestFilterResultBuilder()
                        .shortCircuitResponse(new SaslAuthenticateResponseData()
                                .setErrorCode(Errors.NONE.code())
                                .setAuthBytes(result.responseBytes()))
                        .completed();
            }

            case FAILURE -> {
                yield CompletableFuture.completedFuture(
                        handleAuthenticationFailure(result.errorMessage(), handler, filterContext));
            }
        };
    }

    /**
     * Handle authentication failure.
     */
    @NonNull
    private RequestFilterResult handleAuthenticationFailure(
                                                            String errorMessage,
                                                            @NonNull MechanismHandler handler,
                                                            @NonNull FilterContext filterContext) {

        LOGGER.debug("{}: Authentication failed: {}", filterContext.channelDescriptor(), errorMessage);

        // Transition to Failed state
        if (state instanceof State.RequiringAuthenticate authenticating) {
            state = authenticating.nextStateFailure(errorMessage);
        }

        // Dispose handler
        handler.dispose();

        // Notify proxy of authentication failure
        filterContext.clientSaslAuthenticationFailure(handler.mechanismName(), null,
                new IllegalStateException(errorMessage));

        return filterContext.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                        .setErrorMessage(errorMessage)
                        .setAuthBytes(new byte[0]))
                .withCloseConnection()
                .completed()
                .toCompletableFuture()
                .join();
    }

    /**
     * Handle non-SASL requests.
     * <p>
     * Enforces security barrier: only authenticated connections can proceed.
     * </p>
     */
    @NonNull
    private CompletionStage<RequestFilterResult> handleDefaultRequest(
                                                                      @NonNull RequestHeaderData header,
                                                                      @NonNull ApiMessage request,
                                                                      @NonNull FilterContext filterContext) {

        if (state.isAuthenticated()) {
            // Authenticated - forward request
            return filterContext.forwardRequest(header, request);
        }
        else {
            // Not authenticated - reject request and close connection
            LOGGER.debug("{}: Rejecting unauthenticated request: {}",
                    filterContext.channelDescriptor(), request.getClass().getSimpleName());

            return filterContext.requestFilterResultBuilder()
                    .errorResponse(header, request, Errors.SASL_AUTHENTICATION_FAILED.exception())
                    .withCloseConnection()
                    .completed();
        }
    }
}
