/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A record of which api version was used to decode an ApiMessage off the wire
 * @param apiMessage apiMessage
 * @param decodeApiVersion api version used to decode the message
 */
public record DecodedApiMessage(ApiMessage apiMessage, short decodeApiVersion) {}
