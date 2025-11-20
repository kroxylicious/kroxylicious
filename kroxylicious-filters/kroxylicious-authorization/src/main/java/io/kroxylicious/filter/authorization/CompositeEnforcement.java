/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * Enables composition of a Collection of ApiEnforcements with the same type. The intent is to support
 * a single contiguous range of api versions, but with different ApiEnforcement implementations for
 * non-overlapping subranges. ie apply Enforcement A to versions 0->n, and Enforcement B to versions n+1->n+m.
 * @param <Q>
 * @param <S>
 */
class CompositeEnforcement<Q extends ApiMessage, S extends ApiMessage> extends ApiEnforcement<Q, S> {

    private final List<ApiEnforcement<Q, S>> enforcements;
    private final short maxVersion;
    private final short minVersion;

    CompositeEnforcement(Collection<ApiEnforcement<Q, S>> enforcements) {
        maxVersion = (short) enforcements.stream().mapToInt(ApiEnforcement::maxSupportedVersion).max().orElseThrow();
        minVersion = (short) enforcements.stream().mapToInt(ApiEnforcement::minSupportedVersion).min().orElseThrow();
        List<ApiEnforcement<Q, S>> enforcers = new ArrayList<>(maxVersion);
        IntStream.rangeClosed(0, maxVersion).forEach(i -> enforcers.add(null));
        for (ApiEnforcement<Q, S> enforcement : enforcements) {
            IntStream.rangeClosed(enforcement.minSupportedVersion(), enforcement.maxSupportedVersion()).forEach(value -> {
                if (enforcers.get(value) != null) {
                    throw new IllegalArgumentException(String.format("Enforcement value %s is already supported", enforcers.get(value)));
                }
                enforcers.add(value, enforcement);
            });
        }
        IntStream.rangeClosed(minVersion, maxVersion).forEach(value -> {
            if (enforcers.get(value) == null) {
                throw new IllegalStateException("Api version range support must be contiguous");
            }
        });
        this.enforcements = enforcers;
    }

    @Override
    short minSupportedVersion() {
        return minVersion;
    }

    @Override
    short maxSupportedVersion() {
        return maxVersion;
    }

    @Override
    CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header, Q request, FilterContext context, AuthorizationFilter authorizationFilter) {
        ApiEnforcement<Q, S> specificEnforcement = enforcements.get(header.requestApiVersion());
        if (specificEnforcement == null) {
            return context.requestFilterResultBuilder().errorResponse(header, request, Errors.UNKNOWN_SERVER_ERROR.exception()).completed();
        }
        return specificEnforcement.onRequest(header, request, context, authorizationFilter);
    }
}
