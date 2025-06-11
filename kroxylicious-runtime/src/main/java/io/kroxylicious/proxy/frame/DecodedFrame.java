/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A frame that has been decoded (as opposed to an {@link OpaqueFrame}).
 *
 * @param <H> The header type
 * @param <B> The body type
 *
 */
public abstract class DecodedFrame<H extends ApiMessage, B extends ApiMessage>
        extends AbstractApiMessageBasedFrame<H, B> {

    DecodedFrame(short apiVersion, int correlationId, H header, B body) {
        super(apiVersion, correlationId, header, body);
    }

}
