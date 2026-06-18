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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Records the router tags (tagged field {@value TaggingRouterFactory#TAG})
 * present on each PRODUCE request/response body when this filter sees it.
 * <p>
 * Each invocation appends {@code "<name>:<direction>:<tags>"} to a static
 * list, where {@code <tags>} is the ordered list of tag values found.
 * This allows tests to verify that route filters observe the accumulated
 * modifications from all routers that preceded them in the pipeline.
 */
@Plugin(configType = TagCapturingFilterFactory.Config.class)
public class TagCapturingFilterFactory
        implements FilterFactory<TagCapturingFilterFactory.Config, TagCapturingFilterFactory.Config> {

    private static final CopyOnWriteArrayList<String> CAPTURES = new CopyOnWriteArrayList<>();

    public record Config(String name) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public TagCapturingFilter createFilter(FilterFactoryContext context, Config config) {
        return new TagCapturingFilter(config.name());
    }

    public static List<String> drainCaptures() {
        var snapshot = new ArrayList<>(CAPTURES);
        CAPTURES.clear();
        return snapshot;
    }

    public static void reset() {
        CAPTURES.clear();
    }

    public static class TagCapturingFilter implements RequestFilter, ResponseFilter {
        private final String name;

        TagCapturingFilter(String name) {
            this.name = name;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey,
                                                              short apiVersion,
                                                              RequestHeaderData header,
                                                              ApiMessage body,
                                                              FilterContext context) {
            if (apiKey == ApiKeys.PRODUCE) {
                CAPTURES.add(name + ":request:" + extractTags(body));
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
                CAPTURES.add(name + ":response:" + extractTags(body));
            }
            return context.forwardResponse(header, body);
        }

        private static List<String> extractTags(ApiMessage body) {
            return body.unknownTaggedFields().stream()
                    .filter(f -> f.tag() >= TaggingRouterFactory.BASE_TAG && f.tag() < TaggingRouterFactory.BASE_TAG + 100)
                    .map(f -> new String(f.data(), UTF_8))
                    .toList();
        }
    }
}
