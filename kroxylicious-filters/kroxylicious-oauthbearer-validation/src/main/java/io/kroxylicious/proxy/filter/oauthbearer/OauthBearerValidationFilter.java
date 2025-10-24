/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.LoadingCache;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.filter.SaslAuthenticateResponseFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeRequestFilter;
import io.kroxylicious.proxy.filter.oauthbearer.sasl.BackoffStrategy;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.apache.kafka.common.protocol.Errors.ILLEGAL_SASL_STATE;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.SASL_AUTHENTICATION_FAILED;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

/**
 * OauthBearerValidation filter enables a validation on the JWT token received from client before forwarding it to cluster.
 * <p>
 * If the token is not validated, then the request is short-circuited.
 * It reduces resource consumption on the cluster when a client sends too many invalid SASL requests.
 */
public class OauthBearerValidationFilter
        implements SaslHandshakeRequestFilter, SaslAuthenticateRequestFilter,
        SaslAuthenticateResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OauthBearerValidationFilter.class);
    private static final SaslAuthenticationException INVALID_SASL_STATE_EXCEPTION = new SaslAuthenticationException("invalid sasl state");
    private final ScheduledExecutorService executorService;
    private final BackoffStrategy strategy;
    private final LoadingCache<String, AtomicInteger> rateLimiter;
    private final OAuthBearerValidatorCallbackHandler oauthHandler;
    private @Nullable SaslServer saslServer;
    private boolean validateAuthentication = true;
    private @Nullable String authorizationId;

    public OauthBearerValidationFilter(ScheduledExecutorService executorService, SharedOauthBearerValidationContext sharedContext) {
        this.executorService = executorService;
        this.strategy = sharedContext.backoffStrategy();
        this.rateLimiter = sharedContext.rateLimiter();
        this.oauthHandler = sharedContext.oauthHandler();
    }

    /**
     * Init the SaslServer for downstream SASL
     */
    @Override
    public CompletionStage<RequestFilterResult> onSaslHandshakeRequest(short apiVersion, RequestHeaderData header, SaslHandshakeRequestData request,
                                                                       FilterContext context) {
        // in any case, handshake if SASL server is initiated is a protocol violation
        if (this.saslServer != null) {
            LOGGER.debug("SASL error : Handshake request with a not null SASL server");
            return context.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData().setErrorCode(ILLEGAL_SASL_STATE.code()))
                    .withCloseConnection()
                    .completed();
        }
        try {
            if (OAUTHBEARER_MECHANISM.equals(request.mechanism()) && this.validateAuthentication) {
                this.saslServer = Sasl.createSaslServer(OAUTHBEARER_MECHANISM, "kafka", null, null, this.oauthHandler);
            }
            else {
                this.validateAuthentication = false;
            }
        }
        catch (SaslException e) {
            LOGGER.debug("SASL error : {}", e.getMessage(), e);
            notifyThrowable(context, e);
            return context.requestFilterResultBuilder()
                    .shortCircuitResponse(new SaslHandshakeResponseData().setErrorCode(UNKNOWN_SERVER_ERROR.code()))
                    .withCloseConnection()
                    .completed();
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(short apiVersion, RequestHeaderData header,
                                                                          SaslAuthenticateRequestData request, FilterContext context) {
        if (!validateAuthentication) {
            // client is already authenticated or is not using OAUTHBEARER mechanism, we can forward the request to cluster
            return context.forwardRequest(header, request);
        }
        else {
            var server = saslServer;
            if (server == null) {
                SaslAuthenticateResponseData failedResponse = new SaslAuthenticateResponseData()
                        .setErrorCode(ILLEGAL_SASL_STATE.code())
                        .setErrorMessage("Unexpected SASL request");
                LOGGER.debug("SASL invalid state");
                notifyThrowable(context, INVALID_SASL_STATE_EXCEPTION);
                return context.requestFilterResultBuilder().shortCircuitResponse(failedResponse).withCloseConnection().completed();
            }
            this.saslServer = null;

            return authenticate(server, request.authBytes())
                    .thenCompose(bytes -> context.forwardRequest(header, request))
                    .exceptionallyCompose(e -> {
                        if (e.getCause() instanceof SaslAuthenticationException cause) {
                            SaslAuthenticateResponseData failedResponse = new SaslAuthenticateResponseData()
                                    .setErrorCode(SASL_AUTHENTICATION_FAILED.code())
                                    .setErrorMessage(e.getMessage());
                            LOGGER.debug("SASL Authentication failed : {}", e.getMessage(), e);
                            notifyThrowable(context, cause);
                            return context.requestFilterResultBuilder().shortCircuitResponse(failedResponse).withCloseConnection().completed();
                        }
                        else {
                            LOGGER.debug("SASL error : {}", e.getMessage(), e);
                            if (e instanceof CompletionException) {
                                notifyThrowable(context, e.getCause());
                            }
                            else {
                                notifyThrowable(context, e);
                            }
                            return context.requestFilterResultBuilder()
                                    .shortCircuitResponse(
                                            new SaslAuthenticateResponseData()
                                                    .setErrorCode(UNKNOWN_SERVER_ERROR.code()))
                                    .withCloseConnection()
                                    .completed();
                        }
                    });
        }
    }

    private void notifyThrowable(FilterContext context, Throwable e) {
        if (e instanceof Exception ex) {
            context.clientSaslAuthenticationFailure(OAUTHBEARER_MECHANISM, authorizationId, ex);
        }
        else {
            context.clientSaslAuthenticationFailure(OAUTHBEARER_MECHANISM, authorizationId, new RuntimeException(e));
        }
    }

    @Override
    public CompletionStage<ResponseFilterResult> onSaslAuthenticateResponse(short apiVersion, ResponseHeaderData header,
                                                                            SaslAuthenticateResponseData response, FilterContext context) {
        if (response.errorCode() == NONE.code()) {
            this.validateAuthentication = false;
            context.clientSaslAuthenticationSuccess(OAUTHBEARER_MECHANISM, Objects.requireNonNull(authorizationId));
        }
        return context.forwardResponse(header, response);
    }

    private CompletionStage<byte[]> authenticate(SaslServer server, byte[] authBytes) {
        String rateLimiterKey;
        try {
            rateLimiterKey = createCacheKey(authBytes);
        }
        catch (NoSuchAlgorithmException e) {
            return CompletableFuture.failedStage(e);
        }
        Duration delay = strategy.getDelay(rateLimiter.get(rateLimiterKey).get());
        return schedule(() -> {
            try {
                return CompletableFuture.completedStage(doAuthenticate(server, authBytes));
            }
            catch (Exception e) {
                return CompletableFuture.failedStage(e);
            }
        }, delay)
                .whenComplete((bytes, e) -> {
                    if (e != null) {
                        rateLimiter.get(rateLimiterKey).incrementAndGet();
                    }
                    else {
                        rateLimiter.invalidate(rateLimiterKey);
                    }
                });
    }

    private byte[] doAuthenticate(SaslServer server, byte[] authBytes) throws SaslException {
        try {
            byte[] bytes = server.evaluateResponse(authBytes);
            if (!server.isComplete()) {
                // at this step bytes would be a jsonResponseError from SASL server
                throw new SaslAuthenticationException("SASL failed : " + new String(bytes, StandardCharsets.UTF_8));
            }
            this.authorizationId = server.getAuthorizationID();
            return bytes;
        }
        finally {
            server.dispose();
        }
    }

    @SuppressWarnings("java:S1602") // not able to test the scheduled lambda otherwise
    private <A> CompletionStage<A> schedule(Supplier<CompletionStage<A>> operation, Duration duration) {
        if (duration.equals(Duration.ZERO)) {
            return operation.get();
        }
        CompletableFuture<A> future = new CompletableFuture<>();
        executorService.schedule(() -> {
            operation.get().whenComplete((a, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                }
                else {
                    future.complete(a);
                }
            });
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
        return future;
    }

    @VisibleForTesting
    static String createCacheKey(byte[] input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        byte[] hashBytes = digest.digest(input);

        return HexFormat.of().formatHex(hashBytes);

    }

}
