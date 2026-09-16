/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.FilterContext;

/** The same framed SASL exchange is used for initial authentication and KIP-368 reauthentication. */
final class MdsSaslAuthenticator {
    private static final String MECHANISM = "OAUTHBEARER";
    private final Duration expiryMargin;
    private final Clock clock;
    private final MdsBrokerVersions versions = new MdsBrokerVersions();

    record Session(Instant refreshAfter, boolean reauthenticationSupported) {}

    MdsSaslAuthenticator(Duration expiryMargin, Clock clock) {
        this.expiryMargin = expiryMargin;
        this.clock = clock;
    }

    CompletionStage<Session> authenticate(FilterContext context, MdsToken token, BooleanSupplier failed) {
        requireUsableToken(token, failed);
        return versions.check(context).thenCompose(ignored -> exchange(context, token, failed));
    }

    private CompletionStage<Session> exchange(FilterContext context, MdsToken token, BooleanSupplier failed) {
        requireUsableToken(token, failed);
        Instant handshakeStarted = clock.instant();
        return context.<SaslHandshakeResponseData> sendRequest(new RequestHeaderData().setRequestApiVersion((short) 1),
                new SaslHandshakeRequestData().setMechanism(MECHANISM))
                .handle((reply, error) -> {
                    if (error != null) {
                        throw MdsFailure.atStage(MdsFailure.Reason.UPSTREAM_HANDSHAKE, error);
                    }
                    return reply;
                })
                .thenCompose(reply -> {
                    if (reply.errorCode() != Errors.NONE.code() || !reply.mechanisms().contains(MECHANISM)) {
                        throw new MdsFailure(MdsFailure.Reason.UPSTREAM_HANDSHAKE, reply.errorCode());
                    }
                    requireUsableToken(token, failed);
                    Instant authStarted = clock.instant();
                    return context.<SaslAuthenticateResponseData> sendRequest(new RequestHeaderData().setRequestApiVersion((short) 1),
                            new SaslAuthenticateRequestData().setAuthBytes(token.saslResponse()))
                            .handle((auth, error) -> {
                                if (error != null) {
                                    throw MdsFailure.atStage(MdsFailure.Reason.UPSTREAM_AUTHENTICATE, error);
                                }
                                return session(token, auth, handshakeStarted, authStarted, failed);
                            });
                });
    }

    private void requireUsableToken(MdsToken token, BooleanSupplier failed) {
        if (failed.getAsBoolean() || !clock.instant().isBefore(token.expiresAt().minus(expiryMargin))) {
            throw new MdsFailure(MdsFailure.Reason.TOKEN_LIFETIME);
        }
    }

    private Session session(MdsToken token, SaslAuthenticateResponseData auth, Instant handshakeStarted,
                            Instant authStarted, BooleanSupplier failed) {
        if (failed.getAsBoolean() || auth.errorCode() != Errors.NONE.code() || auth.authBytes().length != 0 || auth.sessionLifetimeMs() < 0) {
            throw new MdsFailure(MdsFailure.Reason.UPSTREAM_AUTHENTICATE, auth.errorCode());
        }
        Instant expiry = token.expiresAt();
        boolean reauthenticationSupported = auth.sessionLifetimeMs() > 0;
        if (reauthenticationSupported) {
            Instant brokerExpiry = authStarted.plusMillis(auth.sessionLifetimeMs());
            if (brokerExpiry.isBefore(expiry)) {
                expiry = brokerExpiry;
            }
        }
        Instant refreshAfter = expiry.minus(expiryMargin);
        // Kafka rejects reauthentication attempts less than one second apart. Refuse an
        // unusably short session instead of continuously renewing it or forwarding stale traffic.
        if (!clock.instant().isBefore(refreshAfter) || refreshAfter.isBefore(handshakeStarted.plusSeconds(1))) {
            throw new MdsFailure(MdsFailure.Reason.SESSION_LIFETIME);
        }
        return new Session(refreshAfter, reauthenticationSupported);
    }
}
