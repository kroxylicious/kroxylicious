/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

public interface ApiMessageBasedResponseFrame<B extends ApiMessage> extends ResponseFrame, ApiMessageBasedFrame<ResponseHeaderData, B> {

    default short headerVersion() {
        return apiKey().messageType.responseHeaderVersion(apiVersion());
    }

}
