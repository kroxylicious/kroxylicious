/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.protocollogger;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class ProtocolLoggerFilter implements RequestFilter, ResponseFilter {

    private final Set<ApiKeys> apiKeys;
    private final MessageFormatter formatter;
    private final Level logLevel;
    private final Logger logger;
    private final LogWarningThrottle warningThrottle;

    ProtocolLoggerFilter(Set<ApiKeys> apiKeys, MessageFormatter formatter, Level logLevel, Logger logger, LogWarningThrottle warningThrottle) {
        this.apiKeys = apiKeys;
        this.formatter = formatter;
        this.logLevel = logLevel;
        this.logger = logger;
        this.warningThrottle = warningThrottle;
    }

    @Override
    public boolean shouldHandleRequest(ApiKeys apiKey, short apiVersion) {
        return logger.isEnabledForLevel(logLevel) && apiKeys.contains(apiKey);
    }

    @Override
    public boolean shouldHandleResponse(ApiKeys apiKey, short apiVersion) {
        return logger.isEnabledForLevel(logLevel) && apiKeys.contains(apiKey);
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                          short apiVersion,
                                                          RequestHeaderData header,
                                                          ApiMessage request,
                                                          FilterContext context) {
        try {
            logger.atLevel(logLevel)
                    .addKeyValue("direction", "request")
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("apiVersion", apiVersion)
                    .addKeyValue("clientCorrelationId", header.correlationId())
                    .addKeyValue("clientId", header.clientId())
                    .addKeyValue("sessionId", context.sessionId())
                    .log(() -> buildRequestLogMessage(apiKey, apiVersion, header, request));
        }
        catch (Exception e) {
            try {
                warningThrottle.onFailure(apiKey, apiVersion, e);
            }
            catch (Exception ignored) {
                // the throttle itself failed; nothing more we can safely do here
            }
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                            short apiVersion,
                                                            ResponseHeaderData header,
                                                            ApiMessage response,
                                                            FilterContext context) {
        try {
            logger.atLevel(logLevel)
                    .addKeyValue("direction", "response")
                    .addKeyValue("apiKey", apiKey)
                    .addKeyValue("apiVersion", apiVersion)
                    .addKeyValue("clientCorrelationId", header.correlationId())
                    .addKeyValue("sessionId", context.sessionId())
                    .log(() -> buildResponseLogMessage(apiKey, apiVersion, header, response));
        }
        catch (Exception e) {
            try {
                warningThrottle.onFailure(apiKey, apiVersion, e);
            }
            catch (Exception ignored) {
                // the throttle itself failed; nothing more we can safely do here
            }
        }
        return context.forwardResponse(header, response);
    }

    String buildRequestLogMessage(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request) {
        ObjectNode entry = formatter.formatRequest(apiKey, apiVersion, header, request);
        return MessageFormatter.prettyPrint(entry);
    }

    String buildResponseLogMessage(ApiKeys apiKey, short apiVersion, ResponseHeaderData header, ApiMessage response) {
        ObjectNode entry = formatter.formatResponse(apiKey, apiVersion, header, response);
        return MessageFormatter.prettyPrint(entry);
    }

}
