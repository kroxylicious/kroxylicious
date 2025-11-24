/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

class Passthrough<Q extends ApiMessage, S extends ApiMessage> extends ApiEnforcement<Q, S> {
    static short asShort(int version) {
        if (version < 0) {
            throw new IllegalArgumentException("version cannot be negative");
        }
        short shortVersion = (short) version;
        if (shortVersion != version) {
            throw new IllegalArgumentException("version cannot be represented as a short");
        }
        return shortVersion;
    }

    private final short minSupportedVersion;
    private final short maxSupportedVersion;

    Passthrough(int minSupportedVersion, int maxSupportedVersion) {
        this.minSupportedVersion = asShort(minSupportedVersion);
        this.maxSupportedVersion = asShort(maxSupportedVersion);
        if (maxSupportedVersion < minSupportedVersion) {
            throw new IllegalArgumentException("maxSupportedVersion cannot be less than minSupportedVersion");
        }
    }

    @Override
    short minSupportedVersion() {
        return minSupportedVersion;
    }

    @Override
    short maxSupportedVersion() {
        return maxSupportedVersion;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                   Q request,
                                                   FilterContext context,
                                                   AuthorizationFilter authorizationFilter) {
        return context.forwardRequest(header, request);
    }

    @Override
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     S response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {

        return context.forwardResponse(header, response);
    }
}
