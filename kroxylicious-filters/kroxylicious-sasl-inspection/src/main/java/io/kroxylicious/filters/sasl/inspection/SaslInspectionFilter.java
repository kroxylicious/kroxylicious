/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.filter.SaslAuthenticateResponseFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeRequestFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeResponseFilter;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A SASL inspection filter supporting the {@code PLAIN},
 * {@code SCRAM-SHA-256} and {@code SCRAM-SHA-512} mechanisms only.
 */
class SaslInspectionFilter
        implements
        SaslHandshakeRequestFilter,
        SaslHandshakeResponseFilter,
        SaslAuthenticateRequestFilter,
        SaslAuthenticateResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslInspectionFilter.class);

    private final Config config;

    private enum State {
        /** A SASL handshake request is required. */
        REQUIRING_HANDSHAKE_REQUEST,
        /** We're waiting for a SASL handshake response from the server. */
        AWAITING_HANDSHAKE_RESPONSE,
        /** A SASL authenticate request is required. */
        REQUIRING_AUTHENTICATE_REQUEST,
        /** We're waiting for a SASL authenticate response from the server. */
        AWAITING_AUTHENTICATE_RESPONSE,
        /**
         * Authentication has been successful and a future SASL authenticate request is allowed for reauthentication.
         * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP-368</a>.
         */
        ALLOWING_HANDSHAKE_REQUEST,
        /**
         * Authentication has been successful, but no future SASL authenticate request is allowed.
         * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP-368</a>.
         */
        DISALLOWING_AUTHENTICATE_REQUEST
    }

    public static final String PROBE_UPSTREAM = Mech.PROBE_UPSTREAM.mechanismName();
    private @NonNull State currentState = State.REQUIRING_HANDSHAKE_REQUEST;
    private Mech chosenMechanism;
    private String authorizationIdFromClient;
    private int numAuthenticateSeen;

    /**
     * True if it has been established that client/broker support re-authentication (KIP-368).
     */
    private boolean clientSupportsReauthentication;

    SaslInspectionFilter(Config config) {
        Objects.requireNonNull(config, "config");
        this.config = config;
        this.clientSupportsReauthentication = false;
        resetState(State.REQUIRING_HANDSHAKE_REQUEST);
    }

    private void resetState(State state) {
        Objects.requireNonNull(state, "state");
        currentState = state;
        chosenMechanism = null;
        authorizationIdFromClient = null;
        numAuthenticateSeen = 0;
    }

    private boolean isMechanismEnabled(String mechanism) {
        return config.enabledMechanisms().contains(mechanism);
    }

    @Override
    public CompletionStage<RequestFilterResult> onSaslHandshakeRequest(short apiVersion,
                                                                       RequestHeaderData header,
                                                                       SaslHandshakeRequestData request,
                                                                       FilterContext context) {
        if (currentState == State.REQUIRING_HANDSHAKE_REQUEST
                || currentState == State.ALLOWING_HANDSHAKE_REQUEST) {
            if (isMechanismEnabled(request.mechanism())) {
                this.chosenMechanism = Mech.fromMechanismName(request.mechanism());
                // If we support this mechanism then forward to the server to check whether it does
                LOGGER.atInfo()
                        .setMessage("Client '{}' on channel {} chosen SASL mechanism '{}'")
                        .addArgument(header::clientId)
                        .addArgument(context::channelDescriptor)
                        .addArgument(() -> chosenMechanism.mechanismName())
                        .log();
            }
            else {
                // If we do not support this mechanism then we need to find out what mechanisms the server has enabled
                // so that we eventually return something mutually acceptable to both proxy and server.
                // To do that we send the server a handshake with a bogus mechanism, so it returns its enabled mechanisms
                this.chosenMechanism = Mech.PROBE_UPSTREAM;
                request.setMechanism(this.chosenMechanism.mechanismName());
                LOGGER.atInfo()
                        .setMessage("Client '{}' on channel {} proposes SASL mechanism '{}' {} this filter, proposing mechanism '{}' to server")
                        .addArgument(header::clientId)
                        .addArgument(context::channelDescriptor)
                        .addArgument(request::mechanism)
                        .addArgument(() -> Mech.SUPPORTED_MECHANISMS.contains(request.mechanism()) ? "not enabled for" : "not supported by")
                        .addArgument(() -> this.chosenMechanism)
                        .log();
            }
            currentState = State.AWAITING_HANDSHAKE_RESPONSE;
            return context.forwardRequest(header, request);
        }
        else {
            LOGGER.atInfo()
                    .setMessage("Client '{}' on channel {} sent SaslHandshakeRequest unexpectedly, while in state {}")
                    .addArgument(header::clientId)
                    .addArgument(context::channelDescriptor)
                    .addArgument(() -> this.currentState)
                    .log();
            return context.requestFilterResultBuilder().shortCircuitResponse(
                    new SaslHandshakeResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code()))
                    .withCloseConnection()
                    .completed();
        }
    }

    @Override
    public CompletionStage<ResponseFilterResult> onSaslHandshakeResponse(short apiVersion,
                                                                         ResponseHeaderData header,
                                                                         SaslHandshakeResponseData response,
                                                                         FilterContext context) {
        if (currentState == State.AWAITING_HANDSHAKE_RESPONSE) {
            var servedAgreed = response.errorCode() == Errors.NONE.code();
            if (servedAgreed && !Mech.PROBE_UPSTREAM.equals(this.chosenMechanism)) {
                LOGGER.atInfo()
                        .setMessage("Server accepts proposed SASL mechanism '{}' on channel {}")
                        .addArgument(() -> chosenMechanism.mechanismName())
                        .addArgument(context::channelDescriptor)
                        .log();
                currentState = State.REQUIRING_AUTHENTICATE_REQUEST;
                return context.forwardResponse(header, response);
            }
            else {
                var commonMechanisms = new ArrayList<>(config.enabledMechanisms());
                commonMechanisms.retainAll(response.mechanisms());
                LOGGER.atInfo()
                        .setMessage("Server rejects proposed SASL mechanism '{}' on channel {} with error {}; supports {}; common mechanisms {}")
                        .addArgument(() -> commonMechanisms)
                        .addArgument(context::channelDescriptor)
                        .addArgument(() -> Errors.forCode(response.errorCode()).name())
                        .addArgument(response::mechanisms)
                        .addArgument(() -> commonMechanisms)
                        .log();
                response.setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code())
                        .setMechanisms(commonMechanisms);
                return context.responseFilterResultBuilder()
                        .forward(header, response)
                        .withCloseConnection()
                        .completed();
            }
        }
        else {
            return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
        }
    }

    @Override
    public CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(short apiVersion,
                                                                          RequestHeaderData header,
                                                                          SaslAuthenticateRequestData request,
                                                                          FilterContext context) {
        if (this.currentState == State.REQUIRING_AUTHENTICATE_REQUEST) {
            Objects.requireNonNull(this.chosenMechanism);
            numAuthenticateSeen += 1;
            if (numAuthenticateSeen == 1) {
                this.clientSupportsReauthentication = header.requestApiVersion() > 0; // KIP-368
            }
            if (this.chosenMechanism.requestContainsAuthorizationId(numAuthenticateSeen)) {
                try {
                    this.authorizationIdFromClient = switch (this.chosenMechanism) {
                        case PLAIN, SCRAM_SHA_256, SCRAM_SHA_512 -> this.chosenMechanism.authorizationId(request);
                        default -> throw Errors.UNSUPPORTED_SASL_MECHANISM.exception();
                    };
                    LOGGER.atInfo()
                            .setMessage("Client '{}' on channel {} sent {} authorizationId '{}'; forwarding to server")
                            .addArgument(header::clientId)
                            .addArgument(context::channelDescriptor)
                            .addArgument(() -> this.chosenMechanism)
                            .addArgument(() -> authorizationIdFromClient)
                            .log();
                }
                catch (AuthenticationException e) {
                    LOGGER.atInfo()
                            .setMessage(
                                    "Client '{}' on channel {} sent {} an authorization request containing a SASL response that could not be interpreted; closing connection. Cause message: {}. Raise log level to DEBUG to see the stack.")
                            .addArgument(header::clientId)
                            .addArgument(context::channelDescriptor)
                            .addArgument(e::getMessage)
                            .setCause(LOGGER.isDebugEnabled() ? e : null)
                            .log();
                    return closeConnectionWithShortCircuitResponse(context, new SaslAuthenticateResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setErrorMessage("Cannot extract authorizationId from SASL authenticate request"));
                }
            }
            this.currentState = State.AWAITING_AUTHENTICATE_RESPONSE;
            return context.forwardRequest(header, request);
        }
        else {
            LOGGER.atInfo().setMessage("Client '{}' on channel {} sent SaslAuthenticateRequest unexpectedly, while in state {}")
                    .addArgument(header::clientId)
                    .addArgument(context::channelDescriptor)
                    .addArgument(() -> this.currentState)
                    .log();
            return closeConnectionWithShortCircuitResponse(context, new SaslAuthenticateResponseData()
                    .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                    .setErrorMessage("SaslHandshake has not been performed"));
        }
    }

    @Override
    public CompletionStage<ResponseFilterResult> onSaslAuthenticateResponse(short apiVersion,
                                                                            ResponseHeaderData header,
                                                                            SaslAuthenticateResponseData response,
                                                                            FilterContext context) {
        if (this.currentState == State.AWAITING_AUTHENTICATE_RESPONSE) {
            if (response.errorCode() == Errors.NONE.code()) {
                if (this.chosenMechanism.isLastSaslAuthenticateResponse(numAuthenticateSeen)) {
                    if (this.authorizationIdFromClient == null) {
                        return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
                    }
                    LOGGER.atInfo()
                            .setMessage("Server accepts SASL credentials for client on channel {}, announcing that client has authorizationId {}")
                            .addArgument(context::channelDescriptor)
                            .addArgument(() -> this.authorizationIdFromClient)
                            .log();
                    context.clientSaslAuthenticationSuccess(chosenMechanism.mechanismName(), this.authorizationIdFromClient);
                }

                if (this.chosenMechanism.isFinished(this.numAuthenticateSeen)) {
                    resetState(this.clientSupportsReauthentication ? State.ALLOWING_HANDSHAKE_REQUEST : State.DISALLOWING_AUTHENTICATE_REQUEST);
                }
                else {
                    this.currentState = State.REQUIRING_AUTHENTICATE_REQUEST;
                }
                return context.forwardResponse(header, response);
            }
            else {
                Errors error = Errors.forCode(response.errorCode());
                LOGGER.atInfo()
                        .setMessage("Server rejects SASL credentials with error {} for client on channel {}")
                        .addArgument(error::name)
                        .addArgument(context::channelDescriptor)
                        .log();
                context.clientSaslAuthenticationFailure(chosenMechanism.mechanismName(), this.authorizationIdFromClient, error.exception());
                resetState(State.REQUIRING_HANDSHAKE_REQUEST);
                return context.responseFilterResultBuilder()
                        .forward(header, response)
                        .withCloseConnection()
                        .completed();
            }
        }
        else {
            return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
        }
    }

    @NonNull
    private static CompletionStage<RequestFilterResult> closeConnectionWithShortCircuitResponse(FilterContext context, ApiMessage response) {
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .withCloseConnection()
                .completed();
    }

    @NonNull
    private CompletionStage<ResponseFilterResult> closeConnectionWithResponse(ResponseHeaderData header, ApiMessage response, FilterContext context) {
        LOGGER.error("Unexpected {} response while in state {}. This should not be possible. Closing connection.",
                header.apiKey(),
                currentState);
        return context.responseFilterResultBuilder()
                .forward(header, response)
                .withCloseConnection()
                .completed();
    }

}
