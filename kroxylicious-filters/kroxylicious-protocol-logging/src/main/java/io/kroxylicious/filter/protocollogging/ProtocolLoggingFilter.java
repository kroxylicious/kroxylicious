/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogging;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class ProtocolLoggingFilter implements RequestFilter, ResponseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolLoggingFilter.class);

    private final Set<ApiKeys> apiKeys;
    private final MessageFormatter formatter;
    private final Level logLevel;

    ProtocolLoggingFilter(Set<ApiKeys> apiKeys, MessageFormatter formatter, Level logLevel) {
        this.apiKeys = apiKeys;
        this.formatter = formatter;
        this.logLevel = logLevel;
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return LOGGER.isEnabledForLevel(logLevel) && apiKeys.contains(apiKey);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return LOGGER.isEnabledForLevel(logLevel) && apiKeys.contains(apiKey);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        LOGGER.atLevel(logLevel)
                .addKeyValue("direction", "request")
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("clientCorrelationId", header.correlationId())
                .addKeyValue("clientId", header.clientId())
                .addKeyValue("sessionId", context.sessionId())
                .log(() -> buildRequestLogMessage(apiKey, apiVersion, header, request));
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        LOGGER.atLevel(logLevel)
                .addKeyValue("direction", "response")
                .addKeyValue("apiKey", apiKey)
                .addKeyValue("apiVersion", apiVersion)
                .addKeyValue("clientCorrelationId", header.correlationId())
                .addKeyValue("sessionId", context.sessionId())
                .log(() -> buildResponseLogMessage(apiKey, apiVersion, header, response));
        return context.forwardResponse(header, response);
    }

    String buildRequestLogMessage(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request) {
        return "REQUEST  " + apiKey + " v" + apiVersion
                + "  corr=" + header.correlationId()
                + "  client=" + header.clientId()
                + "\n" + formatter.formatRequest(apiKey, apiVersion, request);
    }

    String buildResponseLogMessage(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage response) {
        return "RESPONSE " + apiKey + " v" + apiVersion
                + "  corr=" + header.correlationId()
                + "\n" + formatter.formatResponse(apiKey, apiVersion, response);
    }

}
