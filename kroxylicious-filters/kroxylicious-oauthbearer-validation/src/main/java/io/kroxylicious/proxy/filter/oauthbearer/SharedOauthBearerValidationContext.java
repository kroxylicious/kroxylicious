/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;

import com.github.benmanes.caffeine.cache.LoadingCache;

import io.kroxylicious.proxy.filter.oauthbearer.sasl.BackoffStrategy;

public record SharedOauthBearerValidationContext(
        OauthBearerValidation.Config config,
        BackoffStrategy backoffStrategy,
        LoadingCache<String, AtomicInteger> rateLimiter,
        OAuthBearerValidatorCallbackHandler oauthHandler
) {
}
