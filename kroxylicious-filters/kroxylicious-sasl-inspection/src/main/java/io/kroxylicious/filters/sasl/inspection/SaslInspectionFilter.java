/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
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
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A filter that performs <a href="https://github.com/kroxylicious/design/blob/main/proposals/004-terminology-for-authentication.md#sasl-passthrough-inspection">SASL passthrough inspection</a>.
 * It does this by looking at the requests and responses to infer the client's identity negotiated by the SASL layer.
 */
class SaslInspectionFilter
        implements
        SaslHandshakeRequestFilter,
        SaslHandshakeResponseFilter,
        SaslAuthenticateRequestFilter,
        SaslAuthenticateResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslInspectionFilter.class);
    private static final ProbingSaslObserver PROBING_SASL_OBSERVER = new ProbingSaslObserver();
    @VisibleForTesting
    static final String PROBE_UPSTREAM = PROBING_SASL_OBSERVER.mechanismName();

    private final Map<String, SaslObserverFactory> observerFactoryMap;

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

    private @NonNull State currentState = State.REQUIRING_HANDSHAKE_REQUEST;
    private SaslObserver chosenMechanism;
    private String authorizationIdFromClient;
    private int numAuthenticateSeen;

    /**
     * True if it has been established that client/broker support re-authentication (KIP-368).
     */
    private boolean clientSupportsReauthentication;

    SaslInspectionFilter(Map<String, SaslObserverFactory> mechanismFactories) {
        Objects.requireNonNull(mechanismFactories, "mechanismFactories");
        this.observerFactoryMap = mechanismFactories;
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

    @Override
    public CompletionStage<RequestFilterResult> onSaslHandshakeRequest(short apiVersion,
                                                                       RequestHeaderData header,
                                                                       SaslHandshakeRequestData request,
                                                                       FilterContext context) {
        if (currentState == State.REQUIRING_HANDSHAKE_REQUEST
                || currentState == State.ALLOWING_HANDSHAKE_REQUEST) {
            var saslObserverFactory = observerFactoryMap.get(request.mechanism());
            if (saslObserverFactory != null) {
                this.chosenMechanism = saslObserverFactory.createObserver();
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
                this.chosenMechanism = PROBING_SASL_OBSERVER;
                request.setMechanism(this.chosenMechanism.mechanismName());
                LOGGER.atInfo()
                        .setMessage("Client '{}' on channel {} proposes SASL mechanism '{}' {} this filter, proposing mechanism '{}' to server")
                        .addArgument(header::clientId)
                        .addArgument(context::channelDescriptor)
                        .addArgument(request::mechanism)
                        .addArgument(() -> observerFactoryMap.containsKey(request.mechanism()) ? "not enabled for" : "not supported by")
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
            if (response.errorCode() == Errors.NONE.code()) {
                return processSuccessfulHandshakeResponse(header, response, context);
            }
            else {
                return processFailedHandshakeResponse(header, response, context);
            }
        }
        else {
            return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
        }
    }

    @NonNull
    private CompletionStage<ResponseFilterResult> processSuccessfulHandshakeResponse(ResponseHeaderData header, SaslHandshakeResponseData response,
                                                                                     FilterContext context) {
        LOGGER.atInfo()
                .setMessage("Server accepts proposed SASL mechanism '{}' on channel {}")
                .addArgument(() -> chosenMechanism.mechanismName())
                .addArgument(context::channelDescriptor)
                .log();
        currentState = State.REQUIRING_AUTHENTICATE_REQUEST;
        return context.forwardResponse(header, response);
    }

    @NonNull
    private CompletionStage<ResponseFilterResult> processFailedHandshakeResponse(ResponseHeaderData header, SaslHandshakeResponseData response,
                                                                                 FilterContext context) {
        var commonMechanisms = new ArrayList<>(observerFactoryMap.keySet());
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
            try {
                var acquiredAuthorizationId = this.chosenMechanism.clientResponse(request.authBytes());
                if (acquiredAuthorizationId) {
                    this.authorizationIdFromClient = this.chosenMechanism.authorizationId();
                    LOGGER.atInfo()
                            .setMessage("Client '{}' on channel {} sent {} authorizationId '{}'; forwarding to server")
                            .addArgument(header::clientId)
                            .addArgument(context::channelDescriptor)
                            .addArgument(() -> this.chosenMechanism)
                            .addArgument(() -> authorizationIdFromClient)
                            .log();
                }
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
            this.chosenMechanism.serverChallenge(response.authBytes());

            if (response.errorCode() == Errors.NONE.code()) {
                return processSuccessfulAuthenticateResponse(header, response, context);
            }
            else {
                return processFailedAuthentication(header, response, context);
            }
        }
        else {
            return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
        }
    }

    @NonNull
    private CompletionStage<ResponseFilterResult> processSuccessfulAuthenticateResponse(ResponseHeaderData header, SaslAuthenticateResponseData response,
                                                                                        FilterContext context) {
        if (this.chosenMechanism.isFinished()) {
            if (this.authorizationIdFromClient == null) {
                return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
            }
            LOGGER.atInfo()
                    .setMessage("Server accepts SASL credentials for client on channel {}, announcing that client has authorizationId {}")
                    .addArgument(context::channelDescriptor)
                    .addArgument(() -> this.authorizationIdFromClient)
                    .log();
            context.clientSaslAuthenticationSuccess(chosenMechanism.mechanismName(), this.authorizationIdFromClient);
            resetState(this.clientSupportsReauthentication ? State.ALLOWING_HANDSHAKE_REQUEST : State.DISALLOWING_AUTHENTICATE_REQUEST);
        }
        else {
            this.currentState = State.REQUIRING_AUTHENTICATE_REQUEST;
        }
        return context.forwardResponse(header, response);
    }

    @NonNull
    private CompletionStage<ResponseFilterResult> processFailedAuthentication(ResponseHeaderData header, SaslAuthenticateResponseData response,
                                                                              FilterContext context) {
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

    private static class ProbingSaslObserver implements SaslObserver {

        @Override
        public String mechanismName() {
            return "PROBE-UPSTREAM";
        }

        @Override
        public boolean clientResponse(byte[] response) {
            throw new IllegalSaslStateException("Probe mechanism does not accept a client response");
        }

        @Override
        public void serverChallenge(byte[] challenge) throws AuthenticationException {
            throw new IllegalSaslStateException("Probe mechanism does not accept a server challenge");
        }

        @Override
        public boolean isFinished() {
            return true;
        }

        @Override
        public String authorizationId() throws AuthenticationException {
            throw new IllegalSaslStateException("Probe mechanism can not produce an authorization id.");
        }
    }
}
