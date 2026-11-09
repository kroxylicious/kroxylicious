/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import io.kroxylicious.kafka.common.message.CreateTopicsRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * Test filter for <a href="https://github.com/kroxylicious/kroxylicious/issues/5312">#5312</a>:
 * allocates a buffer via {@link FilterContext#createByteBufferOutputStream(int)} then drops the
 * request, exercising the path where the frame holding that buffer is discarded rather than forwarded.
 */
public class BufferAllocatingDropFilter implements CreateTopicsRequestFilter {

    public static final AtomicLong DROPPED_COUNT = new AtomicLong();

    @Override
    public CompletionStage<RequestFilterResult> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                      FilterContext context) {
        context.createByteBufferOutputStream(4000);
        DROPPED_COUNT.incrementAndGet();
        return context.requestFilterResultBuilder().drop().completed();
    }
}
