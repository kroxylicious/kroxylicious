/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

public record ResponsePayload(ApiKeys apiKeys,
                              short apiVersion,
                              ApiMessage message,
                              short responseApiVersion) {

    public ResponsePayload(ApiKeys apiKeys, short apiVersion, ApiMessage message) {
        this(apiKeys, apiVersion, message, apiVersion);
    }

    public ResponsePayload(ApiKeys apiKeys, short apiVersion, ApiMessage message, short responseApiVersion) {
        this.apiKeys = apiKeys;
        this.apiVersion = apiVersion;
        this.message = message;
        this.responseApiVersion = responseApiVersion;
    }
}
