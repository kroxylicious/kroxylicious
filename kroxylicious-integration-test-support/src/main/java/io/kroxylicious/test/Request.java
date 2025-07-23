/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

public record Request(ApiKeys apiKeys,
                      short apiVersion,
                      String clientIdHeader,
                      ApiMessage message,
                      short responseApiVersion) {
    public Request(ApiKeys apiKeys,
                   short apiVersion,
                   String clientIdHeader,
                   ApiMessage message) {
        this(apiKeys, apiVersion, clientIdHeader, message, apiVersion);
    }
}
