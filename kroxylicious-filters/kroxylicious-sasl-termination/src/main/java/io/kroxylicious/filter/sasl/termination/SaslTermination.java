/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;
import io.kroxylicious.scram.credentialstore.ScramCredentialStoreService;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

/**
 * FilterFactory for SASL termination.
 * <p>
 * Terminates SASL authentication at the proxy, authenticating clients
 * against pluggable credential stores or token validators without forwarding
 * authentication to the broker.
 * </p>
 */
@Plugin(configType = SaslTerminationConfig.class)
public class SaslTermination implements FilterFactory<SaslTerminationConfig, SaslTermination.SaslTerminationContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslTermination.class);
    static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";

    static final SaslSubjectBuilder DEFAULT_SUBJECT_BUILDER = context -> CompletableFuture
            .completedStage(new Subject(Set.of(new User(context.clientSaslContext().authorizationId()))));

    private final Clock clock;

    /** Constructs a SaslTermination using the system UTC clock. */
    @SuppressWarnings("unused") // ServiceLoader uses this constructor
    public SaslTermination() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    SaslTermination(Clock clock) {
        this.clock = clock;
    }

    /**
     * Context for the SASL termination filter.
     *
     * @param oauthCallbackHandler initialized callback handler for OAUTHBEARER, null if not configured
     * @param oauthMaxAuthBytes maximum auth payload size for OAUTHBEARER
     * @param scramCredentialStores map of SCRAM mechanism to credential store
     * @param scramPhantomIterations map of SCRAM mechanism to phantom user iteration count
     * @param supportedMechanisms set of configured mechanism names
     * @param maxTimeBeforeReauth maximum session lifetime, null if disabled
     * @param clock clock for session expiry computation
     * @param fixedAuthDelay fixed delay applied to all authentication rounds for timing side-channel mitigation
     * @param subjectBuilder builder for constructing the Subject after authentication
     * @param closeables Things to close
     */
    public record SaslTerminationContext(
                                         @Nullable OAuthBearerValidatorCallbackHandler oauthCallbackHandler,
                                         int oauthMaxAuthBytes,
                                         Map<ScramMechanism, ScramCredentialStore> scramCredentialStores,
                                         Map<ScramMechanism, Integer> scramPhantomIterations,
                                         Set<String> supportedMechanisms,
                                         List<AutoCloseable> closeables,
                                         @Nullable Duration maxTimeBeforeReauth,
                                         Clock clock,
                                         Duration fixedAuthDelay,
                                         SaslSubjectBuilder subjectBuilder)
            implements AutoCloseable {
        @Override
        public void close() {
            RuntimeException firstException = null;
            for (var closeable : closeables()) {
                firstException = closeSafely(closeable, firstException);
            }
            if (firstException != null) {
                throw firstException;
            }
        }

        private static @Nullable RuntimeException closeSafely(AutoCloseable closeable,
                                                              @Nullable RuntimeException firstException) {
            try {
                closeable.close();
            }
            catch (Exception e) {
                RuntimeException re;
                if (e instanceof RuntimeException e1) {
                    re = e1;
                }
                else {
                    re = new RuntimeException(e);
                }
                if (firstException == null) {
                    firstException = re;
                }
                else {
                    firstException.addSuppressed(re);
                }
            }
            return firstException;
        }
    }

    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    @Override
    public SaslTerminationContext initialize(
                                             FilterFactoryContext context,
                                             @NonNull SaslTerminationConfig config)
            throws PluginConfigurationException {

        Objects.requireNonNull(config);

        OAuthBearerValidatorCallbackHandler oauthCallbackHandler = null;
        int oauthMaxAuthBytes = OauthBearerMechanismConfig.DEFAULT_MAX_AUTH_BYTES;
        Map<ScramMechanism, ScramCredentialStore> scramCredentialStores = new EnumMap<>(ScramMechanism.class);
        Map<ScramMechanism, Integer> scramPhantomIterations = new EnumMap<>(ScramMechanism.class);
        Set<String> supportedMechanisms = new LinkedHashSet<>();
        Duration fixedAuthDelay = config.effectiveFixedAuthDelay();

        List<AutoCloseable> closeables = new ArrayList<>(2);

        for (MechanismConfig mechanismConfig : config.mechanisms()) {
            supportedMechanisms.add(mechanismConfig.mechanismName());
            switch (mechanismConfig) {
                case OauthBearerMechanismConfig oauthConfig -> {
                    oauthCallbackHandler = initializeOauthBearer(oauthConfig, closeables);
                    oauthMaxAuthBytes = oauthConfig.effectiveMaxAuthBytes();
                }
                case ScramMechanismConfig scramConfig -> {
                    ScramSaslServerProvider.initialize();
                    ScramCredentialStoreService<Object> service = initScramCredentialStoreService(scramConfig, context);
                    closeables.add(service);
                    ScramCredentialStore store = service.buildCredentialStore();
                    ScramMechanism mechanism = ScramMechanism.forMechanismName(mechanismConfig.mechanismName());
                    scramCredentialStores.put(mechanism, store);
                    scramPhantomIterations.put(mechanism, scramConfig.effectivePhantomIterations());
                }
            }
        }

        var builderService = initSubjectBuilderService(context, config);
        SaslSubjectBuilder subjectBuilder;
        if (builderService == null) {
            subjectBuilder = DEFAULT_SUBJECT_BUILDER;
        }
        else {
            closeables.add(builderService);
            subjectBuilder = builderService.build();
        }
        return new SaslTerminationContext(oauthCallbackHandler,
                oauthMaxAuthBytes,
                scramCredentialStores,
                scramPhantomIterations,
                supportedMechanisms,
                closeables,
                config.maxTimeBeforeReauth(),
                clock,
                fixedAuthDelay,
                subjectBuilder);
    }

    private static OAuthBearerValidatorCallbackHandler initializeOauthBearer(
                                                                             OauthBearerMechanismConfig config,
                                                                             List<AutoCloseable> closeables) {
        OAuthBearerSaslServerProvider.initialize();
        String jwksUrl = config.jwksEndpointUrl().toString();

        addAllowedSaslOauthbearerUrl(jwksUrl, closeables);

        OAuthBearerValidatorCallbackHandler callbackHandler = new OAuthBearerValidatorCallbackHandler();
        callbackHandler.configure(
                createOauthSaslConfigMap(config),
                OAUTHBEARER_MECHANISM,
                createDefaultJaasConfig());
        closeables.add(callbackHandler::close);

        LOGGER.atInfo()
                .addKeyValue("jwksEndpointUrl", jwksUrl)
                .log("Initialized OAUTHBEARER mechanism");

        return callbackHandler;
    }

    @SuppressWarnings("unchecked")
    private static ScramCredentialStoreService<Object> initScramCredentialStoreService(ScramMechanismConfig config, FilterFactoryContext context) {
        ScramCredentialStoreService<Object> service = context.pluginInstance(
                ScramCredentialStoreService.class, config.credentialStore());
        service.initialize(config.credentialStoreConfig());
        return service;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static SaslSubjectBuilderService<Object> initSubjectBuilderService(FilterFactoryContext context, SaslTerminationConfig config) {
        if (config.subjectBuilder() == null) {
            LOGGER.atDebug().log("No subjectBuilder configured, using default");
            return null;
        }
        SaslSubjectBuilderService<Object> service = context.pluginInstance(
                SaslSubjectBuilderService.class, config.subjectBuilder());
        service.initialize(config.subjectBuilderConfig());
        return service;
    }

    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    @Override
    public Filter createFilter(FilterFactoryContext context,
                               @NonNull SaslTerminationContext filterContext) {
        return new SaslTerminationFilter(context.filterDispatchExecutor(), filterContext);
    }

    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    @Override
    public void close(@NonNull SaslTerminationContext initializationData) {
        initializationData.close();
    }

    // Serializes our own read-modify-write on the allowed URLs system property.
    // Cannot protect against other code mutating the same property concurrently
    // (Properties.getProperty reads a ConcurrentHashMap without synchronization).
    private static final Object ALLOWED_URLS_LOCK = new Object();

    // Reference counts for URLs added to the system property, guarded by ALLOWED_URLS_LOCK.
    private static final Map<String, Integer> allowedUrlRefCounts = new HashMap<>();

    @VisibleForTesting
    static void addAllowedSaslOauthbearerUrl(String jwksUrl, List<AutoCloseable> closeables) {
        synchronized (ALLOWED_URLS_LOCK) {
            int prev = allowedUrlRefCounts.getOrDefault(jwksUrl, 0);
            allowedUrlRefCounts.put(jwksUrl, prev + 1);
            if (prev == 0) {
                mutateAllowedUrls(urls -> urls.add(jwksUrl));
            }
            closeables.add(() -> {
                synchronized (ALLOWED_URLS_LOCK) {
                    int count = allowedUrlRefCounts.getOrDefault(jwksUrl, 0);
                    if (count <= 1) {
                        allowedUrlRefCounts.remove(jwksUrl);
                        mutateAllowedUrls(urls -> urls.remove(jwksUrl));
                    }
                    else {
                        allowedUrlRefCounts.put(jwksUrl, count - 1);
                    }
                }
            });
        }
    }

    private static void mutateAllowedUrls(Consumer<List<String>> mutation) {
        String property = System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        List<String> urls = Optional.ofNullable(property)
                .map(p -> Arrays.stream(p.split(","))
                        .map(String::trim)
                        .collect(Collectors.toCollection(ArrayList::new)))
                .orElseGet(ArrayList::new);
        mutation.accept(urls);
        if (urls.isEmpty()) {
            System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
        }
        else {
            System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, String.join(",", urls));
        }
    }

    @VisibleForTesting
    static Map<String, Object> createOauthSaslConfigMap(OauthBearerMechanismConfig config) {
        Map<String, Object> saslConfig = new HashMap<>();
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, config.jwksEndpointUrl().toString());

        putIfNotNull(saslConfig, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
                durationMsOrDefault(config.jwksEndpointRefresh(),
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS));
        putIfNotNull(saslConfig, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
                durationMsOrDefault(config.jwksEndpointRetryBackoff(),
                        SaslConfigs.DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS));
        putIfNotNull(saslConfig, SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
                durationMsOrDefault(config.jwksEndpointRetryBackoffMax(),
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
        if (audience.isEmpty()) {
            throw new PluginConfigurationException("expectedAudience must contain at least one non-empty audience value");
        }
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE, audience);
        saslConfig.put(SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER, config.expectedIssuer());

        return saslConfig;
    }

    private static void putIfNotNull(Map<String, Object> map, String key, @Nullable Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private static Long durationMsOrDefault(@Nullable Duration duration, Long defaultValue) {
        return duration != null ? duration.toMillis() : defaultValue;
    }

    private static List<AppConfigurationEntry> createDefaultJaasConfig() {
        return List.of(new AppConfigurationEntry(
                "OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                Map.of()));
    }
}
