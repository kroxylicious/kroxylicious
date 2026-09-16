/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.FilterContext;

/** Negotiates requirements once per upstream connection, before its first SASL exchange. */
final class MdsBrokerVersions {
    private boolean checked;

    CompletionStage<Void> check(FilterContext context) {
        if (checked) {
            return CompletableFuture.completedStage(null);
        }
        // ApiVersions v0 is the baseline discovery protocol, usable before authentication.
        // Do not send another ApiVersions during KIP-368 reauthentication.
        return context.<ApiVersionsResponseData> sendRequest(new RequestHeaderData().setRequestApiVersion((short) 0),
                new ApiVersionsRequestData()).handle((reply, error) -> {
                    if (error != null) {
                        throw MdsFailure.atStage(MdsFailure.Reason.UPSTREAM_VERSIONS, error);
                    }
                    if (reply.errorCode() != Errors.NONE.code()) {
                        throw new MdsFailure(MdsFailure.Reason.UPSTREAM_VERSIONS, reply.errorCode());
                    }
                    requireVersionOne(reply, ApiKeys.SASL_HANDSHAKE);
                    requireVersionOne(reply, ApiKeys.SASL_AUTHENTICATE);
                    checked = true;
                    return null;
                });
    }

    private static void requireVersionOne(ApiVersionsResponseData reply, ApiKeys apiKey) {
        // Handshake v1 selects framed SASL; Authenticate v1 carries sessionLifetimeMs.
        // Brokers missing either v1 are unsupported; silently downgrading loses these guarantees.
        if (reply.apiKeys().stream().noneMatch(v -> v.apiKey() == apiKey.id && v.minVersion() <= 1 && v.maxVersion() >= 1)) {
            throw new MdsFailure(MdsFailure.Reason.UPSTREAM_VERSIONS, Errors.UNSUPPORTED_VERSION.code());
        }
    }
}
