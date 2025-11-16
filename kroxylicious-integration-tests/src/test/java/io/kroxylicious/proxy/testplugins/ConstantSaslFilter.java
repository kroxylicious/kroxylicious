/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class ConstantSaslFilter implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConstantSaslFilter.class);

    private final ConstantSasl.Config config;
    private boolean finished = false;

    public ConstantSaslFilter(ConstantSasl.Config config) {
        this.config = config;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
        if (apiKey == ApiKeys.SASL_HANDSHAKE
                || apiKey == ApiKeys.SASL_AUTHENTICATE) {
            LOGGER.error("{} is not compatible with clients using SASL authentication. Disconnecting!", getClass().getName());
            return context.requestFilterResultBuilder().withCloseConnection().completed();
        }
        if (!finished && apiKey == config.api()) {
            if (config.exceptionClassName() == null) {
                if (config.principalType() == null) {
                    context.clientSaslAuthenticationSuccess(Objects.requireNonNull(config.mechanism()), Objects.requireNonNull(config.authorizedId()));
                }
                else  {
                    try {
                        var principal = Class.forName(config.principalType())
                                .asSubclass(Principal.class)
                                .getDeclaredConstructor(String.class)
                                .newInstance(config.principalName());
                        context.clientSaslAuthenticationSuccess(Objects.requireNonNull(config.mechanism()), new Subject(principal));
                    }
                    catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                }
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
