/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.netty.buffer.ByteBuf;

public interface ApiMessageBasedFrame<H extends ApiMessage, B extends ApiMessage> extends Frame {
    ApiKeys apiKey();

    short headerVersion();

    H header();

    B body();

    @Override
    int estimateEncodedSize();

    @Override
    void encode(ByteBufAccessor out);

    void add(ByteBuf buffer);

    void transferBuffersTo(ApiMessageBasedFrame<?, ?> frame);

    List<ByteBuf> buffers();
}
