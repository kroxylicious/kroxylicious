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
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import io.kroxylicious.UnknownTaggedFields;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilter;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;

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

    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return new TopicIdToNameResponseStamperFilter(context.filterDispatchExecutor());
    }

    static class TopicIdToNameResponseStamperFilter implements RequestFilter, ResponseFilter {

        private final FilterDispatchExecutor filterDispatchExecutor;
        Map<Integer, TopicNameMapping> correlated = new HashMap<>();

        TopicIdToNameResponseStamperFilter(FilterDispatchExecutor filterDispatchExecutor) {
            this.filterDispatchExecutor = filterDispatchExecutor;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            List<String> list = UnknownTaggedFields.unknownTaggedFieldsToStrings(request, TOPIC_ID_TAG).toList();
            if (list.isEmpty()) {
                return context.requestFilterResultBuilder().errorResponse(header, request, new InvalidRequestException("no topic id tag")).withCloseConnection()
                        .completed();
            }

            Set<Uuid> uuids = Arrays.stream(list.getFirst().split(",")).filter(s -> !s.isEmpty()).map(Uuid::fromString).collect(Collectors.toSet());
            return context.topicNames(uuids).thenCompose(topicNames -> {
                if (!filterDispatchExecutor.isInFilterDispatchThread()) {
                    throw new IllegalStateException("work chained to topicNames future should execute in filter dispatch thread");
                }
                correlated.put(header.correlationId(), topicNames);
                return context.forwardRequest(header, request);
            });
        }

        @Override
        public CompletionStage<ResponseFilterResult> onResponse(ApiKeys apiKey, ResponseHeaderData header, ApiMessage response, FilterContext context) {
            TopicNameMapping topicNames = correlated.remove(header.correlationId());
            if (topicNames == null) {
                return context.forwardResponse(header, response);
            }
            else {
                Stream<String> success = topicNames.topicNames().entrySet().stream()
                        .map(uuidStringEntry -> topicNameMapping(uuidStringEntry.getKey(), uuidStringEntry.getValue(), null));
                Stream<String> fail = topicNames.failures().entrySet().stream()
                        .map(topicIdToError -> topicNameMapping(topicIdToError.getKey(), null, topicIdToError.getValue().getError().name()));
                String outcomes = Stream.concat(success, fail).collect(Collectors.joining(","));
                response.unknownTaggedFields().add(new RawTaggedField(TOPIC_NAME_TAG, outcomes.getBytes(StandardCharsets.UTF_8)));
                return context.forwardResponse(header, response);
            }
        }
    }

    public static String topicNameMapping(Uuid topicId, @Nullable String topicName, @Nullable String error) {
        return topicId + "::" + topicName + "::" + error;
    }
}
