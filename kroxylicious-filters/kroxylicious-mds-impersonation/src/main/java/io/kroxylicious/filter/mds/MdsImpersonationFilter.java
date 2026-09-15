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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/** One identity and one upstream authentication exchange per direct-routing connection. */
@SuppressWarnings("removal") // FilterContext still exposes the transitional Subject API.
final class MdsImpersonationFilter implements RequestFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdsImpersonationFilter.class);
    private static final String MECHANISM = "OAUTHBEARER";
    private final Function<String, CompletionStage<MdsToken>> tokens;
    private final Duration expiryMargin;
    private final FilterDispatchExecutor executor;
    private final Clock clock;
    @Nullable
    private CompletionStage<Instant> authentication;
    private boolean failed;

    MdsImpersonationFilter(Function<String, CompletionStage<MdsToken>> tokens,
                           Duration expiryMargin, FilterDispatchExecutor executor, Clock clock) {
        this.tokens = tokens;
        this.expiryMargin = expiryMargin;
        this.executor = executor;
        this.clock = clock;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header,
                                                          ApiMessage request, FilterContext context) {
        if (failed || apiKey == ApiKeys.SASL_HANDSHAKE || apiKey == ApiKeys.SASL_AUTHENTICATE) {
            return close(context);
        }
        try {
            if (authentication == null) {
                var certificate = context.clientTlsContext().flatMap(ClientTlsContext::clientCertificate);
                var user = context.authenticatedSubject().uniquePrincipalOfType(User.class).map(User::name).orElse(null);
                if (certificate.isEmpty() || user == null || user.isBlank() || user.chars().anyMatch(Character::isISOControl)
                        || context.clientSaslContext().isPresent()) {
                    return close(context);
                }
                authentication = executor.completeOnFilterDispatchThread(tokens.apply(user))
                        .thenCompose(token -> authenticate(context, token));
            }
            return authentication.thenCompose(deadline -> {
                if (failed || !clock.instant().isBefore(deadline)) {
                    return close(context);
                }
                return context.forwardRequest(header, request);
            }).exceptionallyCompose(error -> close(context));
        }
        catch (RuntimeException e) {
            return close(context);
        }
    }

    private CompletionStage<Instant> authenticate(FilterContext context, MdsToken token) {
        if (failed || !clock.instant().isBefore(token.expiresAt().minus(expiryMargin))) {
            throw new IllegalStateException("MDS token has insufficient remaining lifetime");
        }
        return context.<SaslHandshakeResponseData> sendRequest(new RequestHeaderData().setRequestApiVersion((short) 1),
                new SaslHandshakeRequestData().setMechanism(MECHANISM))
                .thenCompose(reply -> {
                    if (failed || reply.errorCode() != Errors.NONE.code() || !reply.mechanisms().contains(MECHANISM)) {
                        throw new IllegalStateException("Upstream OAUTHBEARER handshake failed");
                    }
                    Instant authStarted = clock.instant();
                    if (!authStarted.isBefore(token.expiresAt().minus(expiryMargin))) {
                        throw new IllegalStateException("MDS token expired during upstream handshake");
                    }
                    return context.<SaslAuthenticateResponseData> sendRequest(new RequestHeaderData().setRequestApiVersion((short) 1),
                            new SaslAuthenticateRequestData().setAuthBytes(token.saslResponse()))
                            .thenApply(auth -> {
                                if (auth.errorCode() != Errors.NONE.code() || auth.authBytes().length != 0 || auth.sessionLifetimeMs() < 0) {
                                    throw new IllegalStateException("Upstream MDS token authentication failed");
                                }
                                Instant expiry = token.expiresAt();
                                if (auth.sessionLifetimeMs() > 0) {
                                    Instant brokerExpiry = authStarted.plusMillis(auth.sessionLifetimeMs());
                                    if (brokerExpiry.isBefore(expiry)) {
                                        expiry = brokerExpiry;
                                    }
                                }
                                return expiry.minus(expiryMargin);
                            });
                });
    }

    private CompletionStage<RequestFilterResult> close(FilterContext context) {
        if (!failed) {
            LOGGER.atInfo().addKeyValue("sessionId", context.sessionId()).addKeyValue("filter", "MdsImpersonation")
                    .addKeyValue("virtualCluster", context.getVirtualClusterName())
                    .log("Closing connection after MDS authentication rejection, failure or session expiry");
        }
        failed = true;
        return context.requestFilterResultBuilder().withCloseConnection().completed();
    }
}
