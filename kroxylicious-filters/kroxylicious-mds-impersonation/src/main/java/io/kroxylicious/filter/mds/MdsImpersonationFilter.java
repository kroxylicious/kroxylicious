/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/** One identity per direct-routing connection, with upstream MDS authentication and renewal. */
@SuppressWarnings("removal") // FilterContext still exposes the transitional Subject API.
final class MdsImpersonationFilter implements RequestFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MdsImpersonationFilter.class);
    private final Function<String, CompletionStage<MdsToken>> tokens;
    private final MdsSaslAuthenticator authenticator;
    private final FilterDispatchExecutor executor;
    private final Clock clock;
    @Nullable
    private CompletionStage<MdsSaslAuthenticator.Session> authentication;
    @Nullable
    private MdsSaslAuthenticator.Session session;
    @Nullable
    private String authenticatedUser;
    private boolean failed;

    MdsImpersonationFilter(Function<String, CompletionStage<MdsToken>> tokens,
                           Duration expiryMargin, FilterDispatchExecutor executor, Clock clock) {
        this.tokens = tokens;
        this.authenticator = new MdsSaslAuthenticator(expiryMargin, clock);
        this.executor = executor;
        this.clock = clock;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header,
                                                          ApiMessage request, FilterContext context) {
        if (failed) {
            return context.requestFilterResultBuilder().withCloseConnection().completed();
        }
        if (apiKey == ApiKeys.SASL_HANDSHAKE || apiKey == ApiKeys.SASL_AUTHENTICATE) {
            return close(context, new MdsFailure(MdsFailure.Reason.DOWNSTREAM_SASL));
        }
        try {
            if (session != null && authentication == null && clock.instant().isBefore(session.refreshAfter())) {
                return context.forwardRequest(header, request);
            }
            CompletionStage<MdsSaslAuthenticator.Session> ready = authentication;
            if (authentication == null) {
                if (session != null && !session.reauthenticationSupported()) {
                    return close(context, new MdsFailure(MdsFailure.Reason.SESSION_EXPIRED));
                }
                var user = requireUser(context);
                authenticatedUser = user;
                ready = authenticate(context, user);
            }
            // An incomplete result applies runtime backpressure to subsequent requests.
            // Responses already in flight remain on the normal, correlated response path.
            return ready.thenCompose(renewed -> {
                if (failed || !clock.instant().isBefore(renewed.refreshAfter())) {
                    return close(context, new MdsFailure(MdsFailure.Reason.SESSION_EXPIRED));
                }
                return context.forwardRequest(header, request);
            }).exceptionallyCompose(error -> close(context, MdsFailure.safe(error)));
        }
        catch (RuntimeException e) {
            return close(context, MdsFailure.safe(e));
        }
    }

    private String requireUser(FilterContext context) {
        var certificate = context.clientTlsContext().flatMap(ClientTlsContext::clientCertificate);
        var user = context.authenticatedSubject().uniquePrincipalOfType(User.class).map(User::name).orElse(null);
        if (certificate.isEmpty() || user == null || user.isBlank() || user.chars().anyMatch(Character::isISOControl)
                || context.clientSaslContext().isPresent() || (authenticatedUser != null && !authenticatedUser.equals(user))) {
            throw new MdsFailure(MdsFailure.Reason.IDENTITY);
        }
        return user;
    }

    // Both the HTTP completion and Kafka response callbacks run on the filter dispatch thread.
    // The terminal callback consumes failures and completes the shared authentication result.
    @SuppressWarnings("FutureReturnValueIgnored")
    private CompletionStage<MdsSaslAuthenticator.Session> authenticate(FilterContext context, String user) {
        var result = new CompletableFuture<MdsSaslAuthenticator.Session>();
        authentication = result;
        boolean renewal = session != null;
        executor.completeOnFilterDispatchThread(requestToken(user, context))
                .thenCompose(token -> authenticator.authenticate(context, token, () -> failed))
                .whenComplete((renewed, error) -> {
                    authentication = null;
                    MdsMetrics.authentication(context.getVirtualClusterName(), renewal, error == null ? "success" : "failure");
                    if (error != null) {
                        result.completeExceptionally(error);
                    }
                    else {
                        session = renewed;
                        result.complete(renewed);
                    }
                });
        return result;
    }

    private CompletionStage<MdsToken> requestToken(String user, FilterContext context) {
        long started = System.nanoTime();
        CompletionStage<MdsToken> requested;
        try {
            requested = tokens.apply(user);
        }
        catch (RuntimeException e) {
            requested = CompletableFuture.failedStage(e);
        }
        return requested.handle((token, error) -> {
            if (error != null) {
                var safe = MdsFailure.safe(error);
                if (safe.reason() == MdsFailure.Reason.MDS_BACKOFF || safe.reason() == MdsFailure.Reason.MDS_CAPACITY
                        || safe.reason() == MdsFailure.Reason.MDS_CLOSED) {
                    MdsMetrics.rejected(context.getVirtualClusterName(), safe.reason());
                }
                else {
                    MdsMetrics.token(context.getVirtualClusterName(), "failure", started);
                }
                throw safe;
            }
            MdsMetrics.token(context.getVirtualClusterName(), "success", started);
            return token;
        });
    }

    private CompletionStage<RequestFilterResult> close(FilterContext context, MdsFailure failure) {
        if (!failed) {
            MdsMetrics.closed(context.getVirtualClusterName(), failure.reason());
            LOGGER.atInfo().addKeyValue("sessionId", context.sessionId()).addKeyValue("filter", "MdsImpersonation")
                    .addKeyValue("virtualCluster", context.getVirtualClusterName())
                    .addKeyValue("reason", MdsMetrics.label(failure.reason())).addKeyValue("errorCode", failure.code())
                    .addKeyValue("errorType", failure.errorType())
                    .setCause(LOGGER.isDebugEnabled() ? failure : null)
                    .log("Closing connection after MDS authentication failure");
        }
        failed = true;
        return context.requestFilterResultBuilder().withCloseConnection().completed();
    }
}
