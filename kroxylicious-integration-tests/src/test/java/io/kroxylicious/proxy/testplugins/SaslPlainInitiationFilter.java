/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * A minimal SASL initiation filter supporting the {@code PLAIN} mechanism only.
 * It does not support SASL reauthentication (KIP-368).
 * It may not even be secure!
 * This is only used for integration testing and
 * is <strong>NOT INTENDED FOR USE IN PRODUCTION.</strong>
 */
public class SaslPlainInitiationFilter implements RequestFilter, ApiVersionsResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslPlainInitiationFilter.class);

    enum State {
        START,
        AWAITING_SERVER_API_VERSIONS,
        AWAITING_SERVER_SASL_HANDSHAKE,
        AWAITING_SERVER_SASL_AUTHENTICATE,
        FORWARDING
    }

    private final String username;
    private final String password;

    public SaslPlainInitiationFilter(String username, String password) {
        this.username = username;
        this.password = password;
    }

    private State state = State.START;
    private short saslHandshakeVersion = -1;
    private short saslAuthenticateVersion = -1;

    record BufferedRequest(RequestHeaderData header,
                           ApiMessage request,
                           CompletableFuture<RequestFilterResult> fut) {}

    private final List<BufferedRequest> bufferedRequests = new ArrayList<>();

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        LOGGER.info("Received {} request", apiKey);
        return switch (state) {
            case START -> {
                if (apiKey == ApiKeys.API_VERSIONS) {
                    state = State.AWAITING_SERVER_API_VERSIONS;
                    LOGGER.info("Forwarding {} request", apiKey);
                    yield context.forwardRequest(header, request);
                }
                else {
                    state = State.AWAITING_SERVER_API_VERSIONS;
                    LOGGER.info("Sending {} request", apiKey);
                    yield context.sendRequest(
                            requestHeader(ApiKeys.API_VERSIONS, (short) 4),
                            new ApiVersionsRequestData().setClientSoftwareName("kroxylicious").setClientSoftwareVersion("1.0"))
                            .thenCompose(apiVer -> {
                                return context.forwardRequest(header, request);
                            });
                }
            }
            case AWAITING_SERVER_API_VERSIONS, AWAITING_SERVER_SASL_HANDSHAKE, AWAITING_SERVER_SASL_AUTHENTICATE -> {
                CompletableFuture<RequestFilterResult> fut = new CompletableFuture<>();
                bufferedRequests.add(new BufferedRequest(header, request, fut));
                yield fut;
            }
            case FORWARDING -> {
                LOGGER.info("Forwarding {} request", apiKey);
                yield context.forwardRequest(header, request);
            }
        };
    }

    private static RequestHeaderData requestHeader(ApiKeys apiKey, short apiVersion) {
        return new RequestHeaderData()
                .setRequestApiKey(apiKey.id)
                .setRequestApiVersion(apiVersion)
                .setClientId("sasl-initiator");
    }

    @Override
    public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion,
                                                                       ResponseHeaderData header,
                                                                       ApiVersionsResponseData apiVersionsResponse,
                                                                       FilterContext context) {
        short handshakeMaxVersion = apiVersionsResponse.apiKeys().find(ApiKeys.SASL_HANDSHAKE.id).maxVersion();
        short authenticateMaxVersion = apiVersionsResponse.apiKeys().find(ApiKeys.SASL_AUTHENTICATE.id).maxVersion();
        LOGGER.info("Received API_VERSIONS response, maxHandshakeVersion: {}, maxAuthenticateVersion: {}", handshakeMaxVersion, authenticateMaxVersion);
        this.saslHandshakeVersion = (short) Math.max(handshakeMaxVersion, 1);
        this.saslAuthenticateVersion = (short) Math.max(authenticateMaxVersion, 2);
        LOGGER.info("Will use handshakeVersion: {}, authenticateVersion: {}", this.saslHandshakeVersion, this.saslAuthenticateVersion);

        if (state == State.AWAITING_SERVER_API_VERSIONS) {
            LOGGER.info("Sending SASL_HANDSHAKE request");
            state = State.AWAITING_SERVER_SASL_HANDSHAKE;
            return context.<SaslHandshakeResponseData> sendRequest(
                    requestHeader(ApiKeys.SASL_HANDSHAKE, saslHandshakeVersion),
                    new SaslHandshakeRequestData()
                            .setMechanism("PLAIN"))
                    .thenCompose(handshakeResponse -> {
                        if (handshakeResponse.errorCode() == Errors.NONE.code()) {
                            LOGGER.info("Sending SASL_AUTHENTICATE request");
                            state = State.AWAITING_SERVER_SASL_AUTHENTICATE;
                            return context.<SaslAuthenticateResponseData> sendRequest(
                                    requestHeader(ApiKeys.SASL_AUTHENTICATE, saslAuthenticateVersion),
                                    new SaslAuthenticateRequestData()
                                            .setAuthBytes(("\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8)))
                                    .thenCompose(authenticateResponse -> {
                                        if (authenticateResponse.errorCode() == Errors.NONE.code()) {
                                            for (BufferedRequest bufferedRequest : bufferedRequests) {
                                                LOGGER.info("Forwarding buffered {} request", ApiKeys.forId(bufferedRequest.header.apiKey()));
                                                context.forwardRequest(bufferedRequest.header, bufferedRequest.request);
                                            }
                                            state = State.FORWARDING;
                                            LOGGER.info("Forwarding API_VERSIONS response");
                                            return context.forwardResponse(header, apiVersionsResponse);
                                        }
                                        else {
                                            return giveUp(context, "SaslAuthenticate with the server, using mechanism PLAIN, failed with error " + Errors.forCode(authenticateResponse.errorCode()).name());
                                        }
                                    });
                        }
                        else {
                            // handshake failed
                            return giveUp(context, "SaslHandshake with the server failed with error "
                                    + Errors.forCode(handshakeResponse.errorCode()).name()
                                    + " and mechanisms " + handshakeResponse.mechanisms()
                            );
                        }
                    });
        }
        else {
            LOGGER.info("Forwarding API_VERSIONS response");
            return context.forwardResponse(header, apiVersionsResponse);
        }
    }

    private <T> CompletionStage<ResponseFilterResult> giveUp(FilterContext context, String message) {
        LOGGER.error("{}. Disconnecting client.", message);
        if (!bufferedRequests.isEmpty()) {
            BufferedRequest bufferedRequest = bufferedRequests.get(0);
            bufferedRequest.fut().complete(context.requestFilterResultBuilder()
                    .errorResponse(bufferedRequest.header(), bufferedRequest.request(), Errors.UNKNOWN_SERVER_ERROR.exception())
                    .build());
        }
        return context.responseFilterResultBuilder().drop().completed();
    }
}
