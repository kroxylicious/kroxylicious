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
package io.strimzi.kproxy.filter.topicencryption;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;

import io.strimzi.kproxy.api.filter.FetchResponseFilter;
import io.strimzi.kproxy.api.filter.KrpcFilterContext;
import io.strimzi.kproxy.api.filter.KrpcFilterState;
import io.strimzi.kproxy.api.filter.ProduceRequestFilter;

public class TopicEncryption implements ProduceRequestFilter, FetchResponseFilter {

    // TODO to support topic ids in fetch requests we need metadata
    // but other filters will be interested in keeping track of metadata

    @Override
    public KrpcFilterState onProduceRequest(ProduceRequestData request, KrpcFilterContext context) {
        boolean fragmented = false;
        if (fragmented) {
            // TODO forward the fragments
            // TODO context.forwardRequest();
            // drop the original message
            return KrpcFilterState.DROP;
        }
        else {
            return KrpcFilterState.FORWARD;
        }
    }

    @Override
    public KrpcFilterState onFetchResponse(FetchResponseData response, KrpcFilterContext context) {
        for (var topicResponse : response.responses()) {
            String topicName = topicResponse.topic();
            if (topicName == null) {
                topicName = lookupTopic(topicResponse.topicId());
            }
            // TODO the rest of it
        }
        return KrpcFilterState.FORWARD;
    }

    private String lookupTopic(Uuid topicId) {
        // TODO look up the topic name from the TopicNameFilter
        return null;
    }

}
