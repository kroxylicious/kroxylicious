/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.oauthbearer;

import java.util.concurrent.atomic.AtomicInteger;

import  org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;

import com.github.benmanes.caffeine.cache.LoadingCache;

import io.kroxylicious.filter.oauthbearer.sasl.BackoffStrategy;

/**
 * State shared between all the {@link OauthBearerValidationFilter} instances created by a
 * single {@link OauthBearerValidation} factory.
 * @param config The filter configuration, with defaults applied.
 * @param backoffStrategy The strategy used to delay repeated authentication attempts.
 * @param rateLimiter A cache tracking the number of failed authentication attempts per token.
 * @param oauthHandler The callback handler used to validate JWT tokens.
 */
public record SharedOauthBearerValidationContext(
                                                 OauthBearerValidation.Config config,
                                                 BackoffStrategy backoffStrategy,
                                                 LoadingCache<String, AtomicInteger> rateLimiter,
                                                 OAuthBearerValidatorCallbackHandler oauthHandler) {}
