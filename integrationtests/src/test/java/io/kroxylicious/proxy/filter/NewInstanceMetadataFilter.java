/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

public class NewInstanceMetadataFilter implements MetadataRequestFilter, MetadataResponseFilter {

    public static final int FIXED_THROTTLE_TIME = 999;
    public static final String FIXED_CLIENT_ID = "newInstanceClientId";

    @Override
    public void onMetadataRequest(short apiVersion, RequestHeaderData header, MetadataRequestData request, KrpcFilterContext context) {
        RequestHeaderData duplicate = header.duplicate();
        duplicate.setClientId(FIXED_CLIENT_ID);
        context.forwardRequest(duplicate, request.duplicate());
    }

    @Override
    public void onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response, KrpcFilterContext context) {
        MetadataResponseData duplicate = response.duplicate();
        duplicate.setThrottleTimeMs(FIXED_THROTTLE_TIME);
        context.forwardResponse(header.duplicate(), duplicate);
    }
}
