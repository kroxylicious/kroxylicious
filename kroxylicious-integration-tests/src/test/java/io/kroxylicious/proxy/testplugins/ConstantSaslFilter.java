/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class ConstantSaslFilter implements RequestFilter {

    private final ConstantSasl.Config config;
    private boolean finished = false;

    public ConstantSaslFilter(ConstantSasl.Config config) {
        this.config = config;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        if (!finished && apiKey == config.api()) {
            if (config.exceptionClassName() == null) {
                context.clientSaslAuthenticationSuccess(Objects.requireNonNull(config.mechanism()), Objects.requireNonNull(config.authorizedId()));
            }
            else {
                context.clientSaslAuthenticationFailure(
                        config.mechanism(),
                        config.authorizedId(), config.newException(config.exceptionMessage()));
            }
            finished = true;
        }
        return context.forwardRequest(header, request);
    }
}
