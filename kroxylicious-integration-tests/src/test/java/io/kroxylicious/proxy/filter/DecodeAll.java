/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = Void.class)
public class DecodeAll implements FilterFactory<Void, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        return config;
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return new DecodeAllFilter();
    }

    private static class DecodeAllFilter implements RequestFilter, ResponseFilter {
        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            return context.forwardRequest(header, request);
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
            return context.forwardResponse(header, response);
        }
    }
}
