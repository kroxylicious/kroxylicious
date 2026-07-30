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

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.filter.sasl.termination.mechanism.OauthBearerHandlerFactory;
import io.kroxylicious.filter.sasl.termination.mechanism.ScramSha256HandlerFactory;
import io.kroxylicious.filter.sasl.termination.mechanism.ScramSha512HandlerFactory;
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
     */
    public record SaslTerminationContext(
                                         Map<String, MechanismHandlerFactory> handlerFactories,
                                         @Nullable Duration maxTimeBeforeReauth,
                                         Clock clock) {}

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public SaslTerminationContext initialize(
                                             @NonNull FilterFactoryContext context,
                                             @NonNull SaslTerminationConfig config)
            throws PluginConfigurationException {

        Map<String, MechanismHandlerFactory> initializedFactories = new HashMap<>();

        for (Map.Entry<String, MechanismConfig> entry : config.mechanisms().entrySet()) {
            String mechanismName = entry.getKey();
            MechanismConfig mechanismConfig = entry.getValue();

            MechanismHandlerFactory factory = createFactory(mechanismName);
            factory.initialize(mechanismConfig, context, clock);
            initializedFactories.put(mechanismName, factory);
        }

        return new SaslTerminationContext(initializedFactories, config.maxTimeBeforeReauth(), clock);
    }

    @Override
    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "Framework guarantees non-null parameters")
    public SaslTerminationFilter createFilter(
                                              @NonNull FilterFactoryContext context,
                                              @NonNull SaslTerminationContext filterContext) {
        return new SaslTerminationFilter(filterContext);
    }

    private static MechanismHandlerFactory createFactory(String mechanismName) throws PluginConfigurationException {
        return switch (mechanismName) {
            case "SCRAM-SHA-256" -> new ScramSha256HandlerFactory();
            case "SCRAM-SHA-512" -> new ScramSha512HandlerFactory();
            case "OAUTHBEARER" -> new OauthBearerHandlerFactory();
            default -> throw new PluginConfigurationException(
                    "No handler available for mechanism: " + mechanismName +
                            ". Supported mechanisms: SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER");
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
