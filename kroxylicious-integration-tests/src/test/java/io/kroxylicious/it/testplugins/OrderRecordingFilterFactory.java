/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A filter that records invocations in a static list for verifying
 * filter execution order in integration tests. Each invocation appends
 * {@code "<name>:<direction>"} where direction is "request" or "response".
 * <p>
 * Only PRODUCE API calls are recorded to avoid noise from metadata,
 * API versions, and other protocol-level traffic.
 */
@Plugin(configType = OrderRecordingFilterFactory.Config.class)
public class OrderRecordingFilterFactory
        implements FilterFactory<OrderRecordingFilterFactory.Config, OrderRecordingFilterFactory.Config> {

    private static final CopyOnWriteArrayList<String> INVOCATIONS = new CopyOnWriteArrayList<>();

    public record Config(String name) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public OrderRecordingFilter createFilter(FilterFactoryContext context, Config config) {
        return new OrderRecordingFilter(config.name());
    }

    public static List<String> drainInvocations() {
        var snapshot = new ArrayList<>(INVOCATIONS);
        INVOCATIONS.clear();
        return snapshot;
    }

    public static void reset() {
        INVOCATIONS.clear();
    }

    static void record(String name, String direction) {
        INVOCATIONS.add(name + ":" + direction);
    }

    public static class OrderRecordingFilter implements RequestFilter, ResponseFilter {
        private final String name;

        OrderRecordingFilter(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                              short apiVersion,
                                                              RequestHeaderData header,
                                                              ApiMessage body,
                                                              FilterContext context) {
            if (apiKey == ApiKeys.PRODUCE) {
                record(name, "request");
            }
            return context.forwardRequest(header, body);
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey,
                                                                short apiVersion,
                                                                ResponseHeaderData header,
                                                                ApiMessage body,
                                                                FilterContext context) {
            if (apiKey == ApiKeys.PRODUCE) {
                record(name, "response");
            }
            return context.forwardResponse(header, body);
        }
    }
}
