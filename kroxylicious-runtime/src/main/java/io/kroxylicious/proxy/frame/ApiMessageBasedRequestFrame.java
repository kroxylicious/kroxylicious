/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;

public interface ApiMessageBasedRequestFrame<B extends ApiMessage> extends RequestFrame, ApiMessageBasedFrame<RequestHeaderData, B> {

    default short headerVersion() {
        return apiKey().messageType.requestHeaderVersion(apiVersion());
    }

    @Override
    default boolean hasResponse() {
        return !(apiKeyId() == PRODUCE.id && ((ProduceRequestData) body()).acks() == 0);
    }

}
