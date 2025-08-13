/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.UnknownTaggedFields;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Filter that intercepts all requests and if they have an unknownTaggedField with id {@link #TOPIC_ID_TAG}
 * we expect that tag to contain a comma separated set of {@link Uuid} topic ids (in toString form). The Filter will
 * look up the corresponding topic names for those topic ids. Then when the response is intercepted we will add
 * an unknownTaggedField with id {@link #TOPIC_NAME_TAG} containing a comma-separated set of topic names.
 */
@Plugin(configType = Void.class)
public class TopicIdToNameResponseStamper implements FilterFactory<Void, Void> {

    public static final int TOPIC_ID_TAG = 98;
    public static final int TOPIC_NAME_TAG = 99;

    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        return null;
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return new TopicIdToNameResponseStamperFilter();
    }

    public static class TopicIdToNameResponseStamperFilter implements RequestFilter, ResponseFilter {

        Map<Integer, Set<String>> correlated = new HashMap<>();

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            List<String> list = UnknownTaggedFields.unknownTaggedFieldsToStrings(request, TOPIC_ID_TAG).toList();
            if (list.isEmpty()) {
                return context.forwardRequest(header, request);
            }

            Set<Uuid> uuids = Arrays.stream(list.getFirst().split(",")).map(Uuid::fromString).collect(Collectors.toSet());
            return context.getTopicNames(uuids).thenCompose(topicNames -> {
                Set<String> topicNameSet = uuids.stream().map(topicNames::get).collect(Collectors.toSet());
                correlated.put(header.correlationId(), topicNameSet);
                return context.forwardRequest(header, request);
            });
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
            Set<String> topicNames = correlated.remove(header.correlationId());
            if (topicNames == null) {
                return context.forwardResponse(header, response);
            }
            else {
                String collect = topicNames.stream().collect(Collectors.joining(","));
                response.unknownTaggedFields().add(new RawTaggedField(TOPIC_NAME_TAG, collect.getBytes(StandardCharsets.UTF_8)));
                return context.forwardResponse(header, response);
            }
        }
    }
}