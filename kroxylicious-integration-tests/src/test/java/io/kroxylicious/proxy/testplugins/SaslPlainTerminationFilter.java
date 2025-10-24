/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * A minimal SASL termination filter supporting the {@code PLAIN} mechanism only.
 * It does not support SASL reauthentication (KIP-368).
 * It may not even be secure!
 * This is only used for integration testing and
 * is <strong>NOT INTENDED FOR USE IN PRODUCTION.</strong>
 */
public class SaslPlainTerminationFilter
        implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslPlainTerminationFilter.class);

    private final SaslPlainTermination.PasswordVerifier passwordVerifier;

    private SaslServer saslServer;

    public SaslPlainTerminationFilter(SaslPlainTermination.PasswordVerifier passwordVerifier) {
        this.passwordVerifier = passwordVerifier;
    }

    CompletionStage<RequestFilterResult> onSaslHandshakeRequest(SaslHandshakeRequestData request,
                                                                FilterContext context) {
        Errors errorCode;
        AuthenticateCallbackHandler cbh = null;
        List<String> supportedMechanisms;
        if ("PLAIN".equals(request.mechanism())) {
            cbh = this.passwordVerifier;
            errorCode = Errors.NONE;
            supportedMechanisms = List.of();
        }
        else {
            errorCode = Errors.UNSUPPORTED_SASL_MECHANISM;
            supportedMechanisms = List.of("PLAIN");
        }

        if (cbh != null) {
            try {
                saslServer = Sasl.createSaslServer(request.mechanism(), "kafka", null, null, cbh);
                if (saslServer == null) {
                    throw new IllegalStateException("SASL mechanism had no providers: " + request.mechanism());
                }
            }
            catch (SaslException e) {
                errorCode = Errors.ILLEGAL_SASL_STATE;
            }
        }

        return context.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(errorCode.code())
                        .setMechanisms(supportedMechanisms))
                .completed();
    }

    CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(SaslAuthenticateRequestData request,
                                                                   FilterContext context) {
        Errors error;
        String errorMessage;

        try {
            doEvaluateResponse(context, request.authBytes());
            error = Errors.NONE;
            errorMessage = null;
        }
        catch (SaslAuthenticationException e) {
            error = Errors.SASL_AUTHENTICATION_FAILED;
            errorMessage = e.getMessage();
            context.clientSaslAuthenticationFailure(null, null, e);
        }
        catch (SaslException e) {
            error = Errors.ILLEGAL_SASL_STATE;
            errorMessage = "An error occurred";
            context.clientSaslAuthenticationFailure(null, null, e);
        }

        SaslAuthenticateResponseData body = new SaslAuthenticateResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(errorMessage);

        return context.requestFilterResultBuilder().shortCircuitResponse(body).completed();
    }

    private byte[] doEvaluateResponse(FilterContext context,
                                      byte[] authBytes)
            throws SaslException {
        final byte[] bytes;
        try {
            bytes = saslServer.evaluateResponse(authBytes);
        }
        catch (Exception e) {
            LOGGER.debug("{}: Authentication failed", context.channelDescriptor());
            saslServer.dispose();
            if (e instanceof SaslAuthenticationException sae) {
                throw sae;
            }
            else {
                throw new SaslAuthenticationException(e.getMessage());
            }
        }

        if (saslServer.isComplete()) {
            try {
                String authorizationId = saslServer.getAuthorizationID();
                LOGGER.debug("{}: Authentication successful, authorizationId={}", context.channelDescriptor(), authorizationId);
                context.clientSaslAuthenticationSuccess(saslServer.getMechanismName(), authorizationId);
            }
            finally {
                saslServer.dispose();
            }
        }
        return bytes;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        return switch (apiKey) {
            case API_VERSIONS -> context.forwardRequest(header, request);
            case SASL_HANDSHAKE -> onSaslHandshakeRequest((SaslHandshakeRequestData) request, context);
            case SASL_AUTHENTICATE -> onSaslAuthenticateRequest((SaslAuthenticateRequestData) request, context);
            default -> {
                if (context.clientSaslContext().isPresent()) {
                    yield context.forwardRequest(header, request);
                }
                else {
                    yield context.requestFilterResultBuilder()
                            .errorResponse(header, request, Errors.SASL_AUTHENTICATION_FAILED.exception())
                            .withCloseConnection()
                            .completed();
                }
            }
        };
    }
}
