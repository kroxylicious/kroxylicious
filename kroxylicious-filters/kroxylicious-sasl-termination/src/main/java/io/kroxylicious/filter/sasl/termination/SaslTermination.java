/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.filter.sasl.termination.mechanism.OauthBearerHandlerFactory;
import io.kroxylicious.filter.sasl.termination.mechanism.ScramSha256HandlerFactory;
import io.kroxylicious.filter.sasl.termination.mechanism.ScramSha512HandlerFactory;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    static final SaslSubjectBuilder DEFAULT_SUBJECT_BUILDER = context -> CompletableFuture
            .completedStage(new Subject(Set.of(new User(context.clientSaslContext().authorizationId()))));

    private final Clock clock;

    @SuppressWarnings("unused")
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
     * @param handlerFactories map of mechanism name to initialized handler factory
     * @param maxTimeBeforeReauth maximum session lifetime, null if disabled
     * @param clock clock for session expiry computation
     * @param subjectBuilder builder for constructing the Subject after authentication
     */
    public record SaslTerminationContext(
                                         Map<String, MechanismHandlerFactory> handlerFactories,
                                         @Nullable Duration maxTimeBeforeReauth,
                                         Clock clock,
                                         SaslSubjectBuilder subjectBuilder) {}

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public SaslTerminationContext initialize(
                                             @NonNull FilterFactoryContext context,
                                             @NonNull SaslTerminationConfig config)
            throws PluginConfigurationException {

        Map<String, MechanismHandlerFactory> initializedFactories = new HashMap<>();
        Duration fixedAuthDelay = config.effectiveFixedAuthDelay();

        for (MechanismConfig mechanismConfig : config.mechanisms()) {
            String mechanismName = mechanismConfig.mechanismName();
            MechanismHandlerFactory factory = createFactory(mechanismConfig);
            factory.initialize(mechanismConfig, context, clock, fixedAuthDelay);
            initializedFactories.put(mechanismName, factory);
        }

        SaslSubjectBuilder subjectBuilder = buildSubjectBuilder(context, config);
        return new SaslTerminationContext(initializedFactories, config.maxTimeBeforeReauth(), clock, subjectBuilder);
    }

    @SuppressWarnings("unchecked")
    private static SaslSubjectBuilder buildSubjectBuilder(FilterFactoryContext context, SaslTerminationConfig config) {
        if (config.subjectBuilder() == null) {
            LOGGER.atDebug().log("No subjectBuilder configured, using default");
            return DEFAULT_SUBJECT_BUILDER;
        }
        SaslSubjectBuilderService<Object> service = (SaslSubjectBuilderService<Object>) context.pluginInstance(
                SaslSubjectBuilderService.class, config.subjectBuilder());
        service.initialize(config.subjectBuilderConfig());
        return service.build();
    }

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public SaslTerminationFilter createFilter(
                                              @NonNull FilterFactoryContext context,
                                              @NonNull SaslTerminationContext filterContext) {
        return new SaslTerminationFilter(filterContext);
    }

    private static MechanismHandlerFactory createFactory(MechanismConfig config) {
        return switch (config) {
            case ScramSha256MechanismConfig c -> new ScramSha256HandlerFactory();
            case ScramSha512MechanismConfig c -> new ScramSha512HandlerFactory();
            case OauthBearerMechanismConfig c -> new OauthBearerHandlerFactory();
        };
    }

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public void close(@NonNull SaslTerminationContext initializationData) {
        RuntimeException firstException = null;
        for (var factory : initializationData.handlerFactories().values()) {
            try {
                factory.close();
            }
            catch (RuntimeException e) {
                if (firstException == null) {
                    firstException = e;
                }
                else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
