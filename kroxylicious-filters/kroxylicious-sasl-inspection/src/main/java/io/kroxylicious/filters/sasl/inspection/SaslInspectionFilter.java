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

import edu.umd.cs.findbugs.annotations.Nullable;

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

    private State currentState = State.start();

    SaslInspectionFilter(Map<String, SaslObserverFactory> mechanismFactories) {
        Objects.requireNonNull(mechanismFactories, "mechanismFactories");
        this.observerFactoryMap = mechanismFactories;
    }

    @Override
    public CompletionStage<RequestFilterResult> onSaslHandshakeRequest(short apiVersion,
                                                                       RequestHeaderData header,
                                                                       SaslHandshakeRequestData request,
                                                                       FilterContext context) {
        if (currentState instanceof State.ExpectingHandshakeRequestState handshakeRequestState) {
            var saslObserverFactory = observerFactoryMap.get(request.mechanism());
            SaslObserver saslObserver;
            if (saslObserverFactory != null) {
                saslObserver = saslObserverFactory.createObserver();
                // If we support this mechanism then forward to the server to check whether it does
                LOGGER.atInfo()
                        .setMessage("Client '{}' on channel {} chosen SASL mechanism '{}'")
                        .addArgument(header::clientId)
                        .addArgument(context::channelDescriptor)
                        .addArgument(saslObserver::mechanismName)
                        .log();
            }
            else {
                // If we do not support this mechanism then we need to find out what mechanisms the server has enabled
                // so that we eventually return something mutually acceptable to both proxy and server.
                // To do that we send the server a handshake with a bogus mechanism, so it returns its enabled mechanisms
                saslObserver = PROBING_SASL_OBSERVER;
                request.setMechanism(saslObserver.mechanismName());
                LOGGER.atInfo()
                        .setMessage("Client '{}' on channel {} proposes SASL mechanism '{}' {} this filter, proposing mechanism '{}' to server")
                        .addArgument(header::clientId)
                        .addArgument(context::channelDescriptor)
                        .addArgument(request::mechanism)
                        .addArgument(() -> observerFactoryMap.containsKey(request.mechanism()) ? "not enabled for" : "not supported by")
                        .addArgument(saslObserver::mechanismName)
                        .log();
            }
            currentState = handshakeRequestState.nextState(saslObserver);
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
        if (currentState instanceof State.AwaitingHandshakeResponse awaitingHandshakeResponse) {
            if (response.errorCode() == Errors.NONE.code()) {
                return processSuccessfulHandshakeResponse(header, response, context, awaitingHandshakeResponse);
            }
            else {
                return processFailedHandshakeResponse(header, response, context);
            }
        }
        else {
            return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
        }
    }

    private CompletionStage<ResponseFilterResult> processSuccessfulHandshakeResponse(ResponseHeaderData header, SaslHandshakeResponseData response,
                                                                                     FilterContext context, State.AwaitingHandshakeResponse currentState) {
        LOGGER.atInfo()
                .setMessage("Server accepts proposed SASL mechanism '{}' on channel {}")
                .addArgument(currentState.saslObserver().mechanismName())
                .addArgument(context::channelDescriptor)
                .log();
        this.currentState = currentState.nextState();
        return context.forwardResponse(header, response);
    }

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
        if (this.currentState instanceof State.RequiringAuthenticateRequest state) {
            try {
                var acquiredAuthorizationId = state.saslObserver().clientResponse(request.authBytes());
                if (acquiredAuthorizationId) {
                    LOGGER.atInfo()
                            .setMessage("Client '{}' on channel {} sent {} authorizationId '{}'; forwarding to server")
                            .addArgument(header::clientId)
                            .addArgument(context::channelDescriptor)
                            .addArgument(() -> state.saslObserver().authorizationId())
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
                        .setErrorMessage("Proxy cannot extract authorizationId from SASL authenticate request"));
            }

            this.currentState = state.nextState(apiVersion > 0);
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
        if (this.currentState instanceof State.AwaitingAuthenticateResponse state) {
            state.saslObserver().serverChallenge(response.authBytes());

            if (response.errorCode() == Errors.NONE.code()) {
                return processSuccessfulAuthenticateResponse(header, response, context, state);
            }
            else {
                return processFailedAuthentication(header, response, context, state);
            }
        }
        else {
            return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);
        }
    }

    private CompletionStage<ResponseFilterResult> processSuccessfulAuthenticateResponse(ResponseHeaderData header, SaslAuthenticateResponseData response,
                                                                                        FilterContext context, State.AwaitingAuthenticateResponse state) {
        SaslObserver saslObserver = state.saslObserver();
        if (saslObserver.isFinished()) {
            String authorizationIdFromClient = getAuthorizationIdOrNull(saslObserver);
            if (authorizationIdFromClient == null) {
                // Ordinarily, we never expect to be here. The sasl negotiation ought to be yielded a authorizationId before
                // the negotiation is finished.
                return closeConnectionWithResponse(header, response.setErrorCode(Errors.ILLEGAL_SASL_STATE.code()), context);

            }

            LOGGER.atInfo()
                    .setMessage("Server accepts SASL credentials for client on channel {}, announcing that client has authorizationId {}")
                    .addArgument(context::channelDescriptor)
                    .addArgument(authorizationIdFromClient)
                    .log();
            context.clientSaslAuthenticationSuccess(saslObserver.mechanismName(), authorizationIdFromClient);

        }
        currentState = state.nextState(saslObserver.isFinished());
        return context.forwardResponse(header, response);
    }

    private CompletionStage<ResponseFilterResult> processFailedAuthentication(ResponseHeaderData header, SaslAuthenticateResponseData response,
                                                                              FilterContext context, State.AwaitingAuthenticateResponse state) {
        Errors error = Errors.forCode(response.errorCode());
        LOGGER.atInfo()
                .setMessage("Server rejects SASL credentials with error {} for client on channel {}")
                .addArgument(error::name)
                .addArgument(context::channelDescriptor)
                .log();
        var authorizedId = getAuthorizationIdOrNull(state.saslObserver());
        context.clientSaslAuthenticationFailure(state.saslObserver().mechanismName(), authorizedId, error.exception());
        return context.responseFilterResultBuilder()
                .forward(header, response)
                .withCloseConnection()
                .completed();
    }

    @Nullable
    private static String getAuthorizationIdOrNull(SaslObserver saslObserver) {
        try {
            return saslObserver.authorizationId();
        }
        catch (AuthenticationException | IllegalStateException e) {
            return null;
        }
    }

    private static CompletionStage<RequestFilterResult> closeConnectionWithShortCircuitResponse(FilterContext context, ApiMessage response) {
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .withCloseConnection()
                .completed();
    }

    private CompletionStage<ResponseFilterResult> closeConnectionWithResponse(ResponseHeaderData header, ApiMessage response, FilterContext context) {
        LOGGER.error(
                "Unexpected {} response while in state {}. This may indicate an incorrectly implemented broker that does not conform to https://kafka.apache.org/protocol#sasl_handshake. Closing connection.",
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
