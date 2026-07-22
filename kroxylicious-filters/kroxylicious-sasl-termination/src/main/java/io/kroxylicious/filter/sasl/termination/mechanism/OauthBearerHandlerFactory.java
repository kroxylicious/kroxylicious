/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.sasl.termination.MechanismConfig;
import io.kroxylicious.filter.sasl.termination.OauthBearerMechanismConfig;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

/**
 * Factory for creating OAUTHBEARER mechanism handlers.
 * <p>
 * Manages the {@link OAuthBearerValidatorCallbackHandler} lifecycle and
 * creates per-connection handlers that validate JWT bearer tokens against
 * a JWKS endpoint.
 * </p>
 */
public class OauthBearerHandlerFactory implements MechanismHandlerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(OauthBearerHandlerFactory.class);
    static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";

    static {
        OAuthBearerSaslServerProvider.initialize();
    }

    @Nullable
    private OAuthBearerValidatorCallbackHandler callbackHandler;
    @Nullable
    private Runnable urlCleanupTask;
    private Clock clock = Clock.systemUTC();

    @Override
    public String mechanismName() {
        return OAUTHBEARER_MECHANISM;
    }

    @Override
    public void initialize(MechanismConfig config, FilterFactoryContext context, Clock clock) throws PluginConfigurationException {
        this.clock = clock;
        if (!(config instanceof OauthBearerMechanismConfig oauthConfig)) {
            throw new PluginConfigurationException(
                    "OAUTHBEARER requires OAuth bearer mechanism configuration with jwksEndpointUrl");
        }

        String jwksUrl = oauthConfig.jwksEndpointUrl().toString();
        setAllowedSaslOauthbearerUrl(jwksUrl);

        this.callbackHandler = new OAuthBearerValidatorCallbackHandler();
        callbackHandler.configure(
                createSaslConfigMap(oauthConfig),
                OAUTHBEARER_MECHANISM,
                createDefaultJaasConfig());

        LOGGER.atInfo()
                .addKeyValue("jwksEndpointUrl", jwksUrl)
                .log("Initialized OAUTHBEARER mechanism handler");
    }

    @Override
    public MechanismHandler createHandler() {
        return new OauthBearerHandler(callbackHandler, clock);
    }

    @Override
    public void close() {
        if (callbackHandler != null) {
            callbackHandler.close();
            callbackHandler = null;
        }
        if (urlCleanupTask != null) {
            urlCleanupTask.run();
            urlCleanupTask = null;
        }
    }

    private void setAllowedSaslOauthbearerUrl(String jwksUrl) {
        var allowedUrls = parseAllowedSaslOauthBearerProperty();
        if (!allowedUrls.contains(jwksUrl)) {
            allowedUrls.add(jwksUrl);
            setOrClearAllowedSaslOauthBearerProperty(allowedUrls);
            this.urlCleanupTask = () -> {
                var current = parseAllowedSaslOauthBearerProperty();
                current.remove(jwksUrl);
                setOrClearAllowedSaslOauthBearerProperty(current);
            };
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

    private Map<String, Object> createSaslConfigMap(OauthBearerMechanismConfig config) {
        Map<String, Object> saslConfig = new HashMap<>();
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, config.jwksEndpointUrl().toString());

        putIfNotNull(saslConfig, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
                defaultIfNullOrNegative(config.jwksEndpointRefreshMs(),
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS));
        putIfNotNull(saslConfig, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
                defaultIfNullOrNegative(config.jwksEndpointRetryBackoffMs(),
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS));
        putIfNotNull(saslConfig, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
                defaultIfNullOrNonPositive(config.jwksEndpointRetryBackoffMaxMs(),
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS));

        String scopeClaimName = config.scopeClaimName();
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
                scopeClaimName != null && !scopeClaimName.isBlank() ? scopeClaimName
                        : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);

        String subClaimName = config.subClaimName();
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME,
                subClaimName != null && !subClaimName.isBlank() ? subClaimName
                        : SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME);

        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS, BrokerJwtValidator.class.getName());

        List<String> audience = Arrays.stream(config.expectedAudience().split(","))
                .map(String::trim)
                .filter(element -> !element.isEmpty())
                .toList();
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, audience);
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, config.expectedIssuer());

        return saslConfig;
    }

    private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private static Long defaultIfNullOrNegative(Long value, Long defaultValue) {
        return (value != null && value >= 0L) ? value : defaultValue;
    }

    private static Long defaultIfNullOrNonPositive(Long value, Long defaultValue) {
        return (value != null && value > 0L) ? value : defaultValue;
    }

    private List<AppConfigurationEntry> createDefaultJaasConfig() {
        return List.of(new AppConfigurationEntry(
                "OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                Map.of()));
    }
}
