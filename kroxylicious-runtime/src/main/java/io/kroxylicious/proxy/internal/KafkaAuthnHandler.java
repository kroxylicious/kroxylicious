/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.SaslInternalConfigs;
import org.apache.kafka.common.security.plain.internals.PlainSaslServerProvider;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import io.kroxylicious.proxy.frame.BareSaslRequest;
import io.kroxylicious.proxy.frame.BareSaslResponse;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * <p>A Netty handler that allows the proxy
 * to perform the Kafka SASL authentication exchanges needed by
 * a client connection, specifically {@code SaslHandshake},
 * and {@code SaslAuthenticate}.</p>
 *
 * <p>See the doc for {@link State} for a detailed state machine.</p>
 *
 * <p>Client software and authorization information thus obtained is propagated via
 * an {@link AuthenticationEvent} to upstream handlers, specifically {@link KafkaProxyFrontendHandler}, to use in
 * deciding how the connection to an upstream connection should be made.</p>
 *
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=51809888">KIP-12: Kafka Sasl/Kerberos and SSL implementation</a>
 * added support for Kerberos authentication"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-43%3A+Kafka+SASL+enhancements">KIP-43: Kafka SASL enhancements</a>
 * added the SaslHandshake RPC in Kafka 0.10.0.0"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-84%3A+Support+SASL+SCRAM+mechanisms">KIP-84: Support SASL SCRAM mechanisms</a>
 * added support for the SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-152+-+Improve+diagnostics+for+SASL+authentication+failures">KIP-152: Improve diagnostics for SASL authentication failures</a>
 * added support for the SaslAuthenticate RPC (previously the auth bytes were not encapsulated in a Kafka frame"
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=75968876">KIP-255: OAuth Authentication via SASL/OAUTHBEARER</a>
 * added support for OAUTH authentication"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP-365: Allow SASL Connections to Periodically Re-Authenticate</a>
 * added time-based reauthentication requirements for clients"
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-684+-+Support+mutual+TLS+authentication+on+SASL_SSL+listeners">KIP-684: Support mTLS authentication on SASL_SSL listeners</a>
 * added support for mutual TLS authentication even on SASL_SSL listeners (which was previously ignored)"
 */
public class KafkaAuthnHandler extends ChannelInboundHandlerAdapter {

    private static final IllegalSaslStateException NOT_AUTHENTICATED_EXCEPTION = new IllegalSaslStateException("Not authenticated");

    static {
        PlainSaslServerProvider.initialize();
        ScramSaslServerProvider.initialize();
    }

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuthnHandler.class);

    /**
     * Represents a state in the {@link KafkaAuthnHandler} state machine.
     * <pre><code>
     *                            START
     *                              │
     *       ╭────────────────┬─────┴───────╮───────────────╮
     *       │                │             │               │
     *       ↓                ↓             │               ↓
     * API_VERSIONS ──→ SASL_HANDSHAKE_v0 ══╪══⇒ unframed_SASL_AUTHENTICATE
     *   │      │                           │                     │
     *   │      │             ╭─────────────╯                     │
     *   │      │             ↓                                   │
     *   │      ╰───→ SASL_HANDSHAKE_v1+ ──→ SASL_AUTHENTICATE    │
     *   │                                         │              ↓
     *   ╰─────────────────────────────────────────╰─────→ UPSTREAM_CONNECT
     * </code></pre>
     * <ul>
     * <li>API_VERSIONS if optional within the Kafka protocol</li>
     * <li>SASL authentication may or may not be in use</li>
     * </ul>
     */
    enum State {
        START,
        API_VERSIONS,
        SASL_HANDSHAKE_v0,
        SASL_HANDSHAKE_v1_PLUS,
        UNFRAMED_SASL_AUTHENTICATE,
        FRAMED_SASL_AUTHENTICATE,
        FAILED,
        AUTHN_SUCCESS
    }

    public enum SaslMechanism {
        PLAIN("PLAIN", null) {
            @Override
            public Map<String, Object> negotiatedProperties(SaslServer saslServer) {
                return Map.of();
            }
        },
        SCRAM_SHA_256(
                "SCRAM-SHA-256",
                ScramMechanism.SCRAM_SHA_256
        ) {
            @Override
            public Map<String, Object> negotiatedProperties(SaslServer saslServer) {
                Object lifetime = saslServer.getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY);
                return lifetime == null
                        ? Map.of()
                        : Map.of(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY, lifetime);
            }
        },
        SCRAM_SHA_512("SCRAM-SHA-512", ScramMechanism.SCRAM_SHA_512) {
            @Override
            public Map<String, Object> negotiatedProperties(SaslServer saslServer) {
                Object lifetime = saslServer.getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY);
                return lifetime == null
                        ? Map.of()
                        : Map.of(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY, lifetime);
            }
        };

        // TODO support OAUTHBEARER, GSSAPI
        private final String name;
        private final ScramMechanism scramMechanism;

        SaslMechanism(String saslName, ScramMechanism scramMechanism) {
            this.name = saslName;
            this.scramMechanism = scramMechanism;
        }

        public String mechanismName() {
            return name;
        }

        static SaslMechanism fromMechanismName(String mechanismName) {
            switch (mechanismName) {
                case "PLAIN":
                    return PLAIN;
                case "SCRAM-SHA-256":
                    return SCRAM_SHA_256;
                case "SCRAM-SHA-512":
                    return SCRAM_SHA_512;
            }
            throw new UnsupportedSaslMechanismException(mechanismName);
        }

        public ScramMechanism scramMechanism() {
            return scramMechanism;
        }

        public abstract Map<String, Object> negotiatedProperties(SaslServer saslServer);
    }

    private final List<String> enabledMechanisms;

    @VisibleForTesting
    SaslServer saslServer;

    private final Map<String, AuthenticateCallbackHandler> mechanismHandlers;

    @VisibleForTesting
    State lastSeen;
    private final KafkaProxyExceptionMapper exceptionHandler;

    public KafkaAuthnHandler(
            Channel ch,
            Map<SaslMechanism, AuthenticateCallbackHandler> mechanismHandlers,
            KafkaProxyExceptionMapper exceptionHandler
    ) {
        this(ch, State.START, mechanismHandlers, exceptionHandler);
    }

    @VisibleForTesting
    KafkaAuthnHandler(
            Channel ch,
            State init,
            Map<SaslMechanism, AuthenticateCallbackHandler> mechanismHandlers,
            KafkaProxyExceptionMapper exceptionHandler
    ) {
        this.lastSeen = init;
        this.exceptionHandler = exceptionHandler;
        LOG.debug("{}: Initial state {}", ch, lastSeen);
        this.mechanismHandlers = mechanismHandlers.entrySet()
                                                  .stream()
                                                  .collect(
                                                          Collectors.toMap(
                                                                  e -> e.getKey().mechanismName(),
                                                                  Map.Entry::getValue
                                                          )
                                                  );
        this.enabledMechanisms = List.copyOf(this.mechanismHandlers.keySet());
    }

    private InvalidRequestException illegalTransition(State next) {
        InvalidRequestException e = new InvalidRequestException("Illegal state transition from " + lastSeen + " to " + next);
        lastSeen = State.FAILED;
        return e;
    }

    private void doTransition(Channel channel, State next) {
        State previous = lastSeen;
        switch (next) {
            case API_VERSIONS:
                if (previous != State.START) {
                    throw illegalTransition(next);
                }
                break;
            case SASL_HANDSHAKE_v0:
            case SASL_HANDSHAKE_v1_PLUS:
                if (previous != State.START
                    && previous != State.API_VERSIONS) {
                    throw illegalTransition(next);
                }
                break;
            case UNFRAMED_SASL_AUTHENTICATE:
                if (previous != State.START
                    && previous != State.SASL_HANDSHAKE_v0
                    && previous != State.UNFRAMED_SASL_AUTHENTICATE) {
                    throw illegalTransition(next);
                }
                break;
            case FRAMED_SASL_AUTHENTICATE:
                if (previous != State.SASL_HANDSHAKE_v1_PLUS
                    && previous != State.FRAMED_SASL_AUTHENTICATE) {
                    throw illegalTransition(next);
                }
                break;
            case AUTHN_SUCCESS:
                if (previous != State.FRAMED_SASL_AUTHENTICATE
                    && previous != State.UNFRAMED_SASL_AUTHENTICATE) {
                    throw illegalTransition(next);
                }
                break;
            case FAILED:
                break;
            default:
                throw illegalTransition(next);
        }
        LOG.debug("{}: Transition from {} to {}", channel, lastSeen, next);
        lastSeen = next;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BareSaslRequest) {
            handleBareRequest(ctx, (BareSaslRequest) msg);
        } else if (msg instanceof DecodedRequestFrame) {
            handleFramedRequest(ctx, (DecodedRequestFrame<?>) msg);
        } else if (lastSeen == State.AUTHN_SUCCESS) {
            ctx.fireChannelRead(msg);
        } else {
            throw new IllegalStateException("Unexpected message " + msg.getClass());
        }
    }

    private void handleFramedRequest(ChannelHandlerContext ctx, DecodedRequestFrame<?> frame) throws SaslException {
        switch (frame.apiKey()) {
            case API_VERSIONS:
                if (lastSeen != State.AUTHN_SUCCESS) {
                    doTransition(ctx.channel(), State.API_VERSIONS);
                }
                ctx.fireChannelRead(frame);
                return;
            case SASL_HANDSHAKE:
                doTransition(ctx.channel(), frame.apiVersion() == 0 ? State.SASL_HANDSHAKE_v0 : State.SASL_HANDSHAKE_v1_PLUS);
                onSaslHandshakeRequest(ctx, (DecodedRequestFrame<SaslHandshakeRequestData>) frame);
                return;
            case SASL_AUTHENTICATE:
                doTransition(ctx.channel(), State.FRAMED_SASL_AUTHENTICATE);
                onSaslAuthenticateRequest(ctx, (DecodedRequestFrame<SaslAuthenticateRequestData>) frame);
                return;
            default:
                if (lastSeen == State.AUTHN_SUCCESS) {
                    ctx.fireChannelRead(frame);
                } else {
                    writeFramedResponse(ctx, frame, exceptionHandler.errorResponseMessage(frame, NOT_AUTHENTICATED_EXCEPTION));
                }
        }
    }

    private void handleBareRequest(ChannelHandlerContext ctx, BareSaslRequest msg) throws SaslException {
        // TODO support SASL GSS API
        if (lastSeen == State.SASL_HANDSHAKE_v0
            || lastSeen == State.UNFRAMED_SASL_AUTHENTICATE) {
            doTransition(ctx.channel(), State.UNFRAMED_SASL_AUTHENTICATE);
            // delegate to the SASL code to read the bytes directly
            writeBareResponse(ctx, doEvaluateResponse(ctx, msg.bytes()));
        } else {
            lastSeen = State.FAILED;
            throw new InvalidRequestException("Bare SASL bytes without GSSAPI support or prior SaslHandshake");
        }
    }

    private void writeBareResponse(ChannelHandlerContext ctx, byte[] bytes) throws SaslException {
        ctx.writeAndFlush(new BareSaslResponse(bytes));
    }

    private void onSaslHandshakeRequest(
            ChannelHandlerContext ctx,
            DecodedRequestFrame<SaslHandshakeRequestData> data
    )
      throws SaslException {
        String mechanism = data.body().mechanism();
        Errors error;
        if (lastSeen == State.AUTHN_SUCCESS) {
            error = Errors.ILLEGAL_SASL_STATE;
        } else if (enabledMechanisms.contains(mechanism)) {
            var cbh = mechanismHandlers.get(mechanism);
            // TODO use SNI to supply the correct hostname
            saslServer = Sasl.createSaslServer(mechanism, "kafka", null, null, cbh);
            if (saslServer == null) {
                throw new IllegalStateException("SASL mechanism had no providers: " + mechanism);
            } else {
                error = Errors.NONE;
            }
        } else {
            error = Errors.UNSUPPORTED_SASL_MECHANISM;
        }

        SaslHandshakeResponseData body = new SaslHandshakeResponseData()
                                                                        .setMechanisms(enabledMechanisms)
                                                                        .setErrorCode(error.code());
        writeFramedResponse(ctx, data, body);
        // Request to read the following request
        ctx.channel().read();
    }

    private void onSaslAuthenticateRequest(
            ChannelHandlerContext ctx,
            DecodedRequestFrame<SaslAuthenticateRequestData> data
    ) {
        byte[] bytes = new byte[0];
        Errors error;
        String errorMessage;

        try {
            bytes = doEvaluateResponse(ctx, data.body().authBytes());
            error = Errors.NONE;
            errorMessage = null;
        }
        catch (SaslAuthenticationException e) {
            error = Errors.SASL_AUTHENTICATION_FAILED;
            errorMessage = e.getMessage();
        }
        catch (SaslException e) {
            error = Errors.SASL_AUTHENTICATION_FAILED;
            errorMessage = "An error occurred";
        }

        SaslAuthenticateResponseData body = new SaslAuthenticateResponseData()
                                                                              .setErrorCode(error.code())
                                                                              .setErrorMessage(errorMessage)
                                                                              .setAuthBytes(bytes);
        // TODO add support for session lifetime
        writeFramedResponse(ctx, data, body);
        ctx.channel().read();
    }

    private static void writeFramedResponse(
            ChannelHandlerContext ctx,
            DecodedRequestFrame<?> data,
            ApiMessage body
    ) {
        ctx.writeAndFlush(
                new DecodedResponseFrame<>(
                        data.apiVersion(),
                        data.correlationId(),
                        new ResponseHeaderData().setCorrelationId(data.correlationId()),
                        body
                )
        );
    }

    private byte[] doEvaluateResponse(
            ChannelHandlerContext ctx,
            byte[] authBytes
    )
      throws SaslException {
        final byte[] bytes;
        try {
            bytes = saslServer.evaluateResponse(authBytes);
        }
        catch (SaslAuthenticationException e) {
            LOG.debug("{}: Authentication failed", ctx.channel());
            doTransition(ctx.channel(), State.FAILED);
            saslServer.dispose();
            throw e;
        }
        catch (Exception e) {
            LOG.debug("{}: Authentication failed", ctx.channel());
            doTransition(ctx.channel(), State.FAILED);
            saslServer.dispose();
            throw new SaslAuthenticationException(e.getMessage());
        }

        if (saslServer.isComplete()) {
            try {
                String authorizationId = saslServer.getAuthorizationID();
                var properties = SaslMechanism.fromMechanismName(saslServer.getMechanismName()).negotiatedProperties(saslServer);
                doTransition(ctx.channel(), State.AUTHN_SUCCESS);
                LOG.debug("{}: Authentication successful, authorizationId={}, negotiatedProperties={}", ctx.channel(), authorizationId, properties);
                ctx.fireUserEventTriggered(new AuthenticationEvent(authorizationId, properties));
            }
            finally {
                saslServer.dispose();
            }
        }
        return bytes;
    }
}
