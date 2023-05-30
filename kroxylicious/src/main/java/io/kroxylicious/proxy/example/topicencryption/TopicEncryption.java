/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.example.topicencryption;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;

public class TopicEncryption implements ProduceRequestFilter, FetchResponseFilter {

    // TODO to support topic ids in fetch requests we need metadata
    // but other filters will be interested in keeping track of metadata

    @Override
    public void onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        boolean fragmented = false;
        if (fragmented) {
            // TODO forward the fragments
            // TODO context.forwardRequest();
            // drop the original message
            return;
        }
        else {
            context.forwardRequest(header, request);
        }
    }

    @Override
    public void onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        for (var topicResponse : response.responses()) {
            String topicName = topicResponse.topic();
            if (topicName == null) {
                topicName = lookupTopic(topicResponse.topicId());
            }
            // TODO the rest of it
        }
        context.forwardResponse(header, response);
    }

    private String lookupTopic(Uuid topicId) {
        // TODO look up the topic name from the TopicNameFilter
        return null;
    }

}
