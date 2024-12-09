/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.function.BiFunction;
import java.util.function.Function;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.test.server.MockServer;

/**
 * Static Factory for KroxyliciousTester implementations
 */
public class KroxyliciousTesters {

    /**
     * Creates a kroxylicious tester for the given KroxyliciousConfigBuilder.
     * This will create and start an in-process kroxylicious instance, it is
     * up to the client to close it.
     * @param builder configuration builder for the kroxylicious instance
     * @return KroxyliciousTester
     */
    public static KroxyliciousTester kroxyliciousTester(ConfigurationBuilder builder) {
        return new KroxyliciousTesterBuilder().setConfigurationBuilder(builder).createDefaultKroxyliciousTester();
    }

    /**
     * Creates a builder of kroxylicious testers with given KroxyliciousConfigBuilder.
     * This will create and start an in-process kroxylicious instance, it is
     * up to the client to close it.
     * @param builder configuration builder for the kroxylicious instance
     * @return KroxyliciousTester
     */
    public static KroxyliciousTesterBuilder newBuilder(ConfigurationBuilder builder) {
        return new KroxyliciousTesterBuilder().setConfigurationBuilder(builder);
    }

    /**
     * Creates a kroxylicious tester for the given KroxyliciousConfigBuilder using
     * a function to build an AutoClosable. This is to enable clients to provide their
     * own custom code to create and start a Kroxylicious instance (for example to
     * create it as a sub-process instead of in-process).
     * @param configurationBuilder configuration builder for the kroxylicious instance
     * @param kroxyliciousFactory factory that takes a KroxyliciousConfig and is responsible for starting a Kroxylicious instance for that config
     * @return KroxyliciousTester
     */
    public static KroxyliciousTester kroxyliciousTester(ConfigurationBuilder configurationBuilder,
                                                        BiFunction<Configuration, Features, AutoCloseable> kroxyliciousFactory) {
        return new KroxyliciousTesterBuilder().setConfigurationBuilder(configurationBuilder).setKroxyliciousFactory(kroxyliciousFactory)
                .setClientFactory((clusterName, defaultClientConfiguration) -> new KroxyliciousClients(defaultClientConfiguration)).createDefaultKroxyliciousTester();
    }

    /**
     * Creates a kroxylicious tester for a kroxylicious instance that is proxying a
     * mock kafka broker. The mock can be configured to respond with an exact API
     * message, and be used to verify what was sent to it.
     * @param configurationForMockBootstrap a function that takes the mock broker's bootstrap server address and returns a KroxyliciousConfigBuilder used to configure an in-process Kroxylicious
     * @return KroxyliciousTester
     */
    public static MockServerKroxyliciousTester mockKafkaKroxyliciousTester(Function<String, ConfigurationBuilder> configurationForMockBootstrap) {
        return mockKafkaKroxyliciousTester(configurationForMockBootstrap, Features.defaultFeatures());
    }

    /**
     * Creates a kroxylicious tester for a kroxylicious instance that is proxying a
     * mock kafka broker. The mock can be configured to respond with an exact API
     * message, and be used to verify what was sent to it.
     * @param configurationForMockBootstrap a function that takes the mock broker's bootstrap server address and returns a KroxyliciousConfigBuilder used to configure an in-process Kroxylicious
     * @param features proxy features, pass {@link Features#defaultFeatures()} to use the defaults
     * @return KroxyliciousTester
     */
    public static MockServerKroxyliciousTester mockKafkaKroxyliciousTester(Function<String, ConfigurationBuilder> configurationForMockBootstrap,
                                                                           Features features) {
        return new MockServerKroxyliciousTester(MockServer.startOnRandomPort(), configurationForMockBootstrap,
                config -> DefaultKroxyliciousTester.spawnProxy(config, features),
                (clusterName, defaultClientConfiguration) -> new KroxyliciousClients(defaultClientConfiguration), null);
    }

}
