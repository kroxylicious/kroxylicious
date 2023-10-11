/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.function.Function;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
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
        return new DefaultKroxyliciousTester(builder);
    }

    /**
     * Creates a kroxylicious tester for the given KroxyliciousConfigBuilder using
     * a function to build an AutoClosable. This is to enable clients to provide their
     * own custom code to create and start a Kroxylicious instance (for example to
     * create it as a sub-process instead of in-process).
     * @param builder configuration builder for the kroxylicious instance
     * @param kroxyliciousFactory factory that takes a KroxyliciousConfig and is responsible for starting a Kroxylicious instance for that config
     * @return KroxyliciousTester
     */
    public static KroxyliciousTester kroxyliciousTester(ConfigurationBuilder builder, Function<Configuration, AutoCloseable> kroxyliciousFactory) {
        return new DefaultKroxyliciousTester(builder, kroxyliciousFactory, (clusterName, bootstrapServers) -> new KroxyliciousClients(bootstrapServers));
    }

    /**
     * Creates a kroxylicious tester for a kroxylicious instance that is proxying a
     * mock kafka broker. The mock can be configured to respond with an exact API
     * message, and be used to verify what was sent to it.
     * @param configurationForMockBootstrap a function that takes the mock broker's bootstrap server address and returns a KroxyliciousConfigBuilder used to configure an in-process Kroxylicious
     * @return KroxyliciousTester
     */
    public static MockServerKroxyliciousTester mockKafkaKroxyliciousTester(Function<String, ConfigurationBuilder> configurationForMockBootstrap) {
        return new MockServerKroxyliciousTester(MockServer.startOnRandomPort(), configurationForMockBootstrap);
    }
}