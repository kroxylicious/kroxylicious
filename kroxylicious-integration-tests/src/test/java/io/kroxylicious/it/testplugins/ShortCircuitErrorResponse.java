/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.internal.KafkaProxyExceptionMapper;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * Responds to all requests with an unknown server exception
 */
@Plugin(configType = ShortCircuitErrorResponse.Config.class)
public class ShortCircuitErrorResponse implements FilterFactory<ShortCircuitErrorResponse.Config, ShortCircuitErrorResponse.ResponseMechanism> {

    private static final String ERROR_MESSAGE = ShortCircuitErrorResponse.class.getName() + ": responding error to all requests";

    private static UnknownServerException exception() {
        return new UnknownServerException(ERROR_MESSAGE);
    }

    public enum ResponseMechanism implements RequestFilter {
        ERROR {
            @Override
            public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request, FilterContext context) {
                return context.requestFilterResultBuilder()
                        .errorResponse(header, request, Errors.UNKNOWN_SERVER_ERROR, ERROR_MESSAGE)
                        .completed();
            }
        },
        SHORTCIRCUIT_MESSAGE {
            @Override
            public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request, FilterContext context) {
                final ApiMessage errorResponseMessage = KafkaProxyExceptionMapper.errorResponseForMessage(header, request, exception());
                return context.requestFilterResultBuilder().shortCircuitResponse(errorResponseMessage).completed();
            }
        },
        SHORTCIRCUIT_MESSAGE_AND_HEADER {
            @Override
            public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage request, FilterContext context) {
                final ApiMessage errorResponseMessage = KafkaProxyExceptionMapper.errorResponseForMessage(header, request, exception());
                final ResponseHeaderData responseHeaders = new ResponseHeaderData();
                responseHeaders.setCorrelationId(header.correlationId());
                return context.requestFilterResultBuilder().shortCircuitResponse(responseHeaders, errorResponseMessage).completed();
            }
        }
    }

    @Override
    public ResponseMechanism initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return config.responseMechanism();
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, ResponseMechanism initializationData) {
        return initializationData;
    }

    public record Config(ResponseMechanism responseMechanism) {

    }
}
