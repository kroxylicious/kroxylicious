/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.oauthbearer.sasl.ExponentialJitterBackoffStrategy;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

@Plugin(configType = OauthBearerValidation.Config.class)
public class OauthBearerValidation implements FilterFactory<OauthBearerValidation.Config, SharedOauthBearerValidationContext> {

    @SuppressWarnings("unused")
    @VisibleForTesting
    public OauthBearerValidation() {
        this.oauthHandler = new OAuthBearerValidatorCallbackHandler();
    }

    public OauthBearerValidation(OAuthBearerValidatorCallbackHandler oauthHandler) {
        this.oauthHandler = oauthHandler;
    }

    private final OAuthBearerValidatorCallbackHandler oauthHandler;

    static {
        OAuthBearerSaslServerProvider.initialize();
    }

    @Override
    @SuppressWarnings("java:S2245") // secure randomization not needed for exponential backoff
    public SharedOauthBearerValidationContext initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        Plugins.requireConfig(this, config);
        Config configWithDefaults = new Config(
                config.jwksEndpointUrl,
                config.jwksEndpointRefreshMs() != null && config.jwksEndpointRefreshMs() >= 0L ? config.jwksEndpointRefreshMs()
                        : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
                config.jwksEndpointRetryBackoffMs() != null && config.jwksEndpointRetryBackoffMs() >= 0L ? config.jwksEndpointRetryBackoffMs()
                        : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
                config.jwksEndpointRetryBackoffMaxMs() != null && config.jwksEndpointRetryBackoffMaxMs() > 0L ? config.jwksEndpointRetryBackoffMaxMs()
                        : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
                config.scopeClaimName() != null && !config.scopeClaimName().trim().isEmpty() ? config.scopeClaimName()
                        : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
                config.subClaimName() != null && !config.subClaimName().trim().isEmpty() ? config.subClaimName() : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME,
                config.authenticateBackOffMaxMs() != null && config.authenticateBackOffMaxMs() >= 0L ? config.authenticateBackOffMaxMs() : 60000L,
                config.authenticateCacheMaxSize() != null && config.authenticateCacheMaxSize() > 0L ? config.authenticateCacheMaxSize() : 1000L,
                config.expectedAudience() != null && !config.expectedAudience().isEmpty() ? config.expectedAudience() : null);
        oauthHandler.configure(
                createSaslConfigMap(configWithDefaults),
                OAUTHBEARER_MECHANISM,
                createDefaultJaasConfig());
        LoadingCache<String, AtomicInteger> rateLimiter = Caffeine.newBuilder()
                .expireAfterWrite(configWithDefaults.authenticateBackOffMaxMs(), TimeUnit.MILLISECONDS)
                .maximumSize(configWithDefaults.authenticateCacheMaxSize())
                .build(key -> new AtomicInteger(0));
        ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                ThreadLocalRandom.current());
        return new SharedOauthBearerValidationContext(configWithDefaults, backoffStrategy, rateLimiter, oauthHandler);
    }

    @NonNull
    @Override
    public OauthBearerValidationFilter createFilter(FilterFactoryContext context, SharedOauthBearerValidationContext sharedContext) {
        return new OauthBearerValidationFilter(context.eventLoop(), sharedContext);
    }

    @Override
    public void close(SharedOauthBearerValidationContext sharedContext) {
        oauthHandler.close();
    }

    public record Config(
                         @JsonProperty(required = true) URI jwksEndpointUrl,
                         @JsonProperty Long jwksEndpointRefreshMs,
                         @JsonProperty Long jwksEndpointRetryBackoffMs,
                         @JsonProperty Long jwksEndpointRetryBackoffMaxMs,
                         @JsonProperty String scopeClaimName,
                         @JsonProperty String subClaimName,
                         @JsonProperty Long authenticateBackOffMaxMs,
                         @JsonProperty Long authenticateCacheMaxSize,
                         @JsonProperty List<String> expectedAudience) {}

    private Map<String, ?> createSaslConfigMap(Config config) {
        return Map.of(
                SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, config.jwksEndpointUrl().toString(),
                SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, config.jwksEndpointRefreshMs(),
                SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, config.jwksEndpointRetryBackoffMs(),
                SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, config.jwksEndpointRetryBackoffMaxMs(),
                SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, config.scopeClaimName(),
                SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, config.subClaimName(),
                SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, config.expectedAudience());
    }

    private List<AppConfigurationEntry> createDefaultJaasConfig() {
        return List.of(new AppConfigurationEntry("OAuthBearerLoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Map.of()));
    }
}