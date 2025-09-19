/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
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

    public static final Set<String> SUPPORTED_MECHANISMS = Set.of(
            Mech.PLAIN.mechanismName(),
            Mech.SCRAM_SHA_256.mechanismName(),
            Mech.SCRAM_SHA_512.mechanismName());

    private final Config config;

    enum State {
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

    enum Mech {

        /**
         * This a bogus SASL mechanism used when the proxy receives a handshake for a mechanism
         * that the proxy doesn't recognise. It sends this bogus mechanism (which we hope won't ever
         * be registered) to elicit from the server the mechanism(s) it supports.
         */
        PROBE_UPSTREAM(0) {
            @Override
            public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
                throw new AuthenticationException("Illegal state: Unexpected method call on " + this + " mech");
            }

            @Override
            public String authorizationId(SaslAuthenticateRequestData request) {
                throw new AuthenticationException("Illegal state: Unexpected method call on " + this + " mech");
            }

            @Override
            public boolean isFinished(int numAuthenticateSeen) {
                throw new AuthenticationException("Illegal state: Unexpected method call on " + this + " mech");
            }
        },

        PLAIN(1) {
            @Override
            public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
                return numAuthenticateSeen == 1;
            }

            /* This function originally copied from Apach Kafka's PlainSaslServer */
            private static List<String> parsePlainClient(String string) {
                /*
                 * Message format (from https://tools.ietf.org/html/rfc4616):
                 *
                 * message = [authzid] UTF8NUL authcid UTF8NUL passwd
                 * authcid = 1*SAFE ; MUST accept up to 255 octets
                 * authzid = 1*SAFE ; MUST accept up to 255 octets
                 * passwd = 1*SAFE ; MUST accept up to 255 octets
                 * UTF8NUL = %x00 ; UTF-8 encoded NUL character
                 *
                 * SAFE = UTF1 / UTF2 / UTF3 / UTF4
                 * ;; any UTF-8 encoded Unicode character except NUL
                 */
                List<String> tokens = new ArrayList<>();
                int startIndex = 0;
                for (int i = 0; i < 4; ++i) {
                    int endIndex = string.indexOf("\u0000", startIndex);
                    if (endIndex == -1) {
                        tokens.add(string.substring(startIndex));
                        break;
                    }
                    tokens.add(string.substring(startIndex, endIndex));
                    startIndex = endIndex + 1;
                }

                if (tokens.size() != 3) {
                    throw new SaslAuthenticationException("Invalid SASL/PLAIN response: expected 3 tokens, got " +
                            tokens.size());
                }

                return tokens;
            }

            @Override
            public String authorizationId(SaslAuthenticateRequestData request) {
                var tokens = parsePlainClient(new String(request.authBytes(), StandardCharsets.UTF_8));
                String authorizationIdFromClient = tokens.get(0);
                String username = tokens.get(1);
                return authorizationIdFromClient.isEmpty() ? username : authorizationIdFromClient;
            }

            @Override
            public boolean isFinished(int numAuthenticateSeen) {
                if (numAuthenticateSeen == 1) {
                    return true;
                }
                throw new AuthenticationException("Illegal state: nextState called on " + this + " mech with numAuthenticateSeen=" + numAuthenticateSeen);
            }
        },

        SCRAM_SHA_256(2) {
            @Override
            public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
                return numAuthenticateSeen == 1;
            }

            @NonNull
            private List<String> parseScramClientFirst(String clientFirst) {
                List<String> tokens = new ArrayList<>(4);
                int startIndex = 0;
                for (int i = 0; i < 4; ++i) {
                    int endIndex = clientFirst.indexOf(",", startIndex);
                    if (endIndex == -1) {
                        tokens.add(clientFirst.substring(startIndex));
                        break;
                    }
                    tokens.add(clientFirst.substring(startIndex, endIndex));
                    startIndex = endIndex + 1;
                }
                if (tokens.size() != 4) {
                    throw new SaslAuthenticationException("Invalid SASL/" + this.mechanismName() + " response: expected 4 tokens, got " +
                            tokens.size());
                }
                return tokens;
            }

            @Override
            public String authorizationId(SaslAuthenticateRequestData request) {
                // n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
                // n,a=ursel,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL

                var clientFirst = new String(request.authBytes(), StandardCharsets.UTF_8);
                List<String> tokens = parseScramClientFirst(clientFirst);
                String username = tokens.get(2).startsWith("n=") ? tokens.get(2).substring(2) : null;
                if (username == null) {
                    throw new SaslAuthenticationException("Invalid SCRAM client first message, username (n) absent");
                }
                String authzid = tokens.get(1).startsWith("a=") ? tokens.get(1).substring(2) : "";
                return ScramFormatter.username(authzid.isEmpty() ? username : authzid);
            }

            @Override
            public boolean isFinished(int numAuthenticateSeen) {
                if (numAuthenticateSeen == 1) {
                    return false;
                }
                if (numAuthenticateSeen == 2) {
                    return true;
                }
                throw new AuthenticationException("Illegal state: nextState called on " + this + " mech with numAuthenticateSeen=" + numAuthenticateSeen);
            }
        },

        SCRAM_SHA_512(2) {
            @Override
            public boolean requestContainsAuthorizationId(int numAuthenticateSeen) {
                // All SCRAM mechs are the same
                return SCRAM_SHA_256.requestContainsAuthorizationId(numAuthenticateSeen);
            }

            @Override
            public String authorizationId(SaslAuthenticateRequestData request) {
                // All SCRAM mechs are the same
                return SCRAM_SHA_256.authorizationId(request);
            }

            @Override
            public boolean isFinished(int numAuthenticateSeen) {
                // All SCRAM mechs are the same
                return SCRAM_SHA_256.isFinished(numAuthenticateSeen);
            }
        };

        private final int numAuthenticateRequests;

        Mech(int numAuthenticateRequests) {
            this.numAuthenticateRequests = numAuthenticateRequests;
        }

        static Mech fromMechanismName(String mechanism) {
            return Mech.valueOf(mechanism.replace('-', '_'));
        }

        boolean isLastSaslAuthenticateResponse(int numAuthenticateSeen) {
            return numAuthenticateSeen == this.numAuthenticateRequests;
        }

        String mechanismName() {
            return this.name().replace('_', '-');
        }

        public abstract boolean isFinished(int numAuthenticateSeen);

        public abstract String authorizationId(SaslAuthenticateRequestData request);

        public abstract boolean requestContainsAuthorizationId(int numAuthenticateSeen);
    }

    public static final String PROBE_UPSTREAM = Mech.PROBE_UPSTREAM.mechanismName();
    private @NonNull State currentState = State.REQUIRING_HANDSHAKE_REQUEST;
    private Mech chosenMechanism;
    private String authorizationIdFromClient;
    private int numAuthenticateSeen;
    private boolean clientSupportsReauthentication;

    SaslInspectionFilter(Config config) {
        Objects.requireNonNull(config, "config");
        this.config = config;
        this.clientSupportsReauthentication = false;
        resetState();
    }

    private void resetState() {
        if (this.clientSupportsReauthentication) {
            currentState = State.ALLOWING_HANDSHAKE_REQUEST;
        }
        else {
            currentState = State.REQUIRING_HANDSHAKE_REQUEST;
        }
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
                        .addArgument(() -> SUPPORTED_MECHANISMS.contains(request.mechanism()) ? "not enabled for" : "not supported by")
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
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Server accepts proposed SASL mechanism '{}' on channel {}",
                            chosenMechanism.mechanismName(),
                            context.channelDescriptor());
                }
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
            }
            currentState = State.REQUIRING_AUTHENTICATE_REQUEST;
            return context.forwardResponse(header, response);
        }
        else {
            return closeConnectionDueToIllegalState(header, context);
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
            if (this.currentState == State.REQUIRING_AUTHENTICATE_REQUEST && numAuthenticateSeen == 1) {
                this.clientSupportsReauthentication = header.requestApiVersion() > 0; // KIP-368
            }
            if (this.chosenMechanism.requestContainsAuthorizationId(numAuthenticateSeen)) {
                try {
                    if (this.chosenMechanism != null) {
                        this.authorizationIdFromClient = switch (this.chosenMechanism) {
                            case PLAIN, SCRAM_SHA_256, SCRAM_SHA_512 -> this.chosenMechanism.authorizationId(request);
                            default -> throw Errors.UNSUPPORTED_SASL_MECHANISM.exception();
                        };
                    }
                    else {
                        throw Errors.UNSUPPORTED_SASL_MECHANISM.exception();
                    }
                    LOGGER.atInfo()
                            .setMessage("Client '{}' on channel {} sent {} authorizationId '{}'; forwarding to server")
                            .addArgument(header::clientId)
                            .addArgument(context::channelDescriptor)
                            .addArgument(() -> this.chosenMechanism)
                            .addArgument(() -> authorizationIdFromClient)
                            .log();
                }
                catch (AuthenticationException e) {
                    // TODO what should we do here, if we failed to decode the request property?
                    return context.forwardRequest(header, request);
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
            return context.requestFilterResultBuilder().shortCircuitResponse(
                    new SaslAuthenticateResponseData()
                            .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                            .setErrorMessage("SaslHandshake has not been performed"))
                    .completed();
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
                        return closeConnectionDueToIllegalState(header, context);
                    }
                    LOGGER.atInfo()
                            .setMessage("Server accepts SASL credentials for client on channel {}")
                            .addArgument(context::channelDescriptor)
                            .log();
                    LOGGER.atInfo()
                            .setMessage("Announcing that client on channel {} has authorizationId {}")
                            .addArgument(context::channelDescriptor)
                            .addArgument(() -> this.authorizationIdFromClient)
                            .log();
                    context.clientSaslAuthenticationSuccess(chosenMechanism.mechanismName(), this.authorizationIdFromClient);
                }

                if (this.chosenMechanism.isFinished(this.numAuthenticateSeen)) {
                    resetState();
                }
                else {
                    this.currentState = State.REQUIRING_AUTHENTICATE_REQUEST;
                }
                return context.forwardResponse(header, response);
            }
            else {
                Errors error = Errors.forCode(response.errorCode());
                LOGGER.info("Server rejects SASL credentials with error {} for client on channel {}",
                        error.name(), context.channelDescriptor());
                context.clientSaslAuthenticationFailure(chosenMechanism.mechanismName(), this.authorizationIdFromClient, error.exception());
                resetState();
                return context.forwardResponse(header, response);
            }
        }
        else {
            return closeConnectionDueToIllegalState(header, context);
        }
    }

    @NonNull
    private CompletionStage<ResponseFilterResult> closeConnectionDueToIllegalState(ResponseHeaderData header, FilterContext context) {
        LOGGER.error("Unexpected {} response while in state {}. This should not be possible. Closing connection.",
                header.apiKey(),
                currentState);
        return context.responseFilterResultBuilder().withCloseConnection().completed();
    }

}
