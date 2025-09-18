/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator;
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
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

@Plugin(configType = OauthBearerValidation.Config.class)
public class OauthBearerValidation implements FilterFactory<OauthBearerValidation.Config, SharedOauthBearerValidationContext> {

    private final OAuthBearerValidatorCallbackHandler oauthHandler;

    @VisibleForTesting
    static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";

    static {
        OAuthBearerSaslServerProvider.initialize();
    }

    private final Deque<Runnable> oauthSystemPropertyCleanupTasks = new ConcurrentLinkedDeque<>();

    @SuppressWarnings("unused")
    public OauthBearerValidation() {
        this(new OAuthBearerValidatorCallbackHandler());
    }

    @VisibleForTesting
    OauthBearerValidation(OAuthBearerValidatorCallbackHandler oauthHandler) {
        this.oauthHandler = oauthHandler;
    }

    @Override
    @SuppressWarnings("java:S2245") // secure randomization not needed for exponential backoff
    public SharedOauthBearerValidationContext initialize(FilterFactoryContext context, @Nullable Config config) throws PluginConfigurationException {
        var cfg = Plugins.requireConfig(this, config);
        setAllowedSaslOauthbearerSysPropIfNecessary(cfg.jwksEndpointUrl().toString());
        Config configWithDefaults = initConfigWithDefaults(cfg);
        oauthHandler.configure(
                createSaslConfigMap(configWithDefaults),
                OAUTHBEARER_MECHANISM,
                createDefaultJaasConfig());
        LoadingCache<String, AtomicInteger> rateLimiter = Caffeine.newBuilder()
                .expireAfterWrite(Objects.requireNonNull(configWithDefaults.authenticateBackOffMaxMs()), TimeUnit.MILLISECONDS)
                .maximumSize(Objects.requireNonNull(configWithDefaults.authenticateCacheMaxSize()))
                .build(key -> new AtomicInteger(0));
        ExponentialJitterBackoffStrategy backoffStrategy = new ExponentialJitterBackoffStrategy(Duration.ofMillis(500), Duration.ofSeconds(5), 2d,
                ThreadLocalRandom.current());
        return new SharedOauthBearerValidationContext(configWithDefaults, backoffStrategy, rateLimiter, oauthHandler);
    }

    private void setAllowedSaslOauthbearerSysPropIfNecessary(String jwkUrl) {
        var allowedUrls = parseAllowedSaslOauthBearerProperty();
        if (!allowedUrls.contains(jwkUrl)) {
            allowedUrls.add(jwkUrl);
            setOrClearAllowedSaslOauthBearerProperty(allowedUrls);
            this.oauthSystemPropertyCleanupTasks.push(() -> {
                var now = parseAllowedSaslOauthBearerProperty();
                now.remove(jwkUrl);
                setOrClearAllowedSaslOauthBearerProperty(now);
            });
        }
    }

    private List<String> parseAllowedSaslOauthBearerProperty() {
        String property = System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        var allowedList = Optional.ofNullable(property)
                .map(p -> Arrays.stream(p.split(","))
                        .map(String::trim)
                        .toList())
                .orElse(List.of());
        return new ArrayList<>(allowedList);
    }

    private void setOrClearAllowedSaslOauthBearerProperty(List<String> allowedUrls) {
        if (allowedUrls.isEmpty()) {
            System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        }
        else {
            System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, String.join(",", allowedUrls));
        }
    }

    @Override
    public OauthBearerValidationFilter createFilter(FilterFactoryContext context, @NonNull SharedOauthBearerValidationContext sharedContext) {
        return new OauthBearerValidationFilter(context.filterDispatchExecutor(), sharedContext);
    }

    @Override
    public void close(SharedOauthBearerValidationContext sharedContext) {
        oauthHandler.close();
        while (!oauthSystemPropertyCleanupTasks.isEmpty()) {
            oauthSystemPropertyCleanupTasks.pop().run();
        }

    }

    public record Config(
                         @JsonProperty(required = true) URI jwksEndpointUrl,
                         @JsonProperty @Nullable Long jwksEndpointRefreshMs,
                         @JsonProperty @Nullable Long jwksEndpointRetryBackoffMs,
                         @JsonProperty @Nullable Long jwksEndpointRetryBackoffMaxMs,
                         @JsonProperty @Nullable String scopeClaimName,
                         @JsonProperty @Nullable String subClaimName,
                         @JsonProperty @Nullable Long authenticateBackOffMaxMs,
                         @JsonProperty @Nullable Long authenticateCacheMaxSize,
                         @JsonProperty @Nullable String expectedAudience,
                         @JsonProperty @Nullable String expectedIssuer,
                         @JsonProperty @Nullable String jwtValidatorClass) {}

    private Map<String, ?> createSaslConfigMap(Config config) {
        Map<String, Object> saslConfig = new HashMap<>();
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, config.jwksEndpointUrl().toString());
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, config.jwksEndpointRefreshMs());
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, config.jwksEndpointRetryBackoffMs());
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, config.jwksEndpointRetryBackoffMaxMs());
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, config.scopeClaimName());
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, config.subClaimName());
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS, config.jwtValidatorClass());
        if (config.expectedAudience() != null) {
            List<String> audience = Arrays.stream(config.expectedAudience().split(","))
                    .map(String::trim)
                    .filter(element -> !element.isEmpty())
                    .toList();
            saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, audience);
        }
        if (config.expectedIssuer() != null) {
            saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, config.expectedIssuer());
        }
        return saslConfig;
    }

    private List<AppConfigurationEntry> createDefaultJaasConfig() {
        return List.of(new AppConfigurationEntry("OAuthBearerLoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Map.of()));
    }

    private Config initConfigWithDefaults(Config config) {
        return new Config(
                config.jwksEndpointUrl,
                defaultIfNullOrNegative(config.jwksEndpointRefreshMs(), SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS),
                defaultIfNullOrNegative(config.jwksEndpointRetryBackoffMs(), SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS),
                defaultIfNullOrNonPositive(config.jwksEndpointRetryBackoffMaxMs(), SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS),
                defaultIfNullOrEmpty(config.scopeClaimName(), SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME),
                defaultIfNullOrEmpty(config.subClaimName(), SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME),
                defaultIfNullOrNegative(config.authenticateBackOffMaxMs(), 60000L),
                defaultIfNullOrNonPositive(config.authenticateCacheMaxSize(), 1000L),
                defaultIfNullOrEmpty(config.expectedAudience(), null),
                defaultIfNullOrEmpty(config.expectedIssuer(), null),
                defaultIfNullOrEmpty(config.jwtValidatorClass(), BrokerJwtValidator.class.getName()));
    }

    private @Nullable Long defaultIfNullOrNegative(@Nullable Long value, @Nullable Long defaultValue) {
        return (value != null && value >= 0L) ? value : defaultValue;
    }

    private @Nullable Long defaultIfNullOrNonPositive(@Nullable Long value, @Nullable Long defaultValue) {
        return (value != null && value > 0L) ? value : defaultValue;
    }

    private @Nullable String defaultIfNullOrEmpty(@Nullable String value, @Nullable String defaultValue) {
        return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
    }
}