/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy.internal.filter.topicname;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;

import io.strimzi.kproxy.filter.CreateTopicsResponseFilter;
import io.strimzi.kproxy.filter.DeleteTopicsResponseFilter;
import io.strimzi.kproxy.filter.KrpcFilterContext;
import io.strimzi.kproxy.filter.KrpcFilterState;
import io.strimzi.kproxy.filter.MetadataResponseFilter;

/**
 * Maintains a local mapping of topic id to topic name
 */
public class TopicNameFilter
        implements MetadataResponseFilter, DeleteTopicsResponseFilter, CreateTopicsResponseFilter {

    private final Map<Uuid, String> topicNames = new HashMap<>();

    @Override
    public KrpcFilterState onMetadataResponse(MetadataResponseData response, KrpcFilterContext context) {
        if (response.topics() != null) {
            for (var topic : response.topics()) {
                topicNames.put(topic.topicId(), topic.name());
            }
        }
        // TODO how can we expose this state to other filters?
        // TODO filterContext.put("topicNames", topicNames);
        return KrpcFilterState.FORWARD;
    }

    // We don't implement DeleteTopicsRequestFilter because we don't know whether
    // a delete topics request will succeed.
    @Override
    public KrpcFilterState onDeleteTopicsResponse(DeleteTopicsResponseData response, KrpcFilterContext context) {
        for (var resp : response.responses()) {
            topicNames.remove(resp.topicId());
        }
        return KrpcFilterState.FORWARD;
    }

    @Override
    public KrpcFilterState onCreateTopicsResponse(CreateTopicsResponseData response, KrpcFilterContext context) {
        for (var topic : response.topics()) {
            topicNames.put(topic.topicId(), topic.name());
        }
        return KrpcFilterState.FORWARD;
    }
}
