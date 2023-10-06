/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.server.MockServer;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KroxyliciousTesterBuilder {

    private String trustStoreLocation = null;
    private String trustStorePassword = null;
    private Function<Configuration, AutoCloseable> kroxyliciousFactory = DefaultKroxyliciousTester::spawnProxy;
    private DefaultKroxyliciousTester.ClientFactory clientFactory = (clusterName, defaultConfiguration) -> new KroxyliciousClients(defaultConfiguration);
    private ConfigurationBuilder configurationBuilder;

    private Function<String, ConfigurationBuilder> mockConfigurationFunction;

    private List<ResponsePayload> mockPayloads = new ArrayList<>();

    public KroxyliciousTesterBuilder setKroxyliciousFactory(Function<Configuration, AutoCloseable> kroxyliciousFactory) {
        this.kroxyliciousFactory = kroxyliciousFactory;
        return this;
    }

    KroxyliciousTesterBuilder setClientFactory(DefaultKroxyliciousTester.ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    public KroxyliciousTesterBuilder setConfigurationBuilder(ConfigurationBuilder configurationBuilder) {
        this.configurationBuilder = configurationBuilder;
        return this;
    }

    public KroxyliciousTesterBuilder setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
        return this;
    }

    public KroxyliciousTesterBuilder setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public KroxyliciousTesterBuilder setMockConfigurationFunction(Function<String, ConfigurationBuilder> mockConfigurationFunction) {
        this.mockConfigurationFunction = mockConfigurationFunction;
        return this;
    }

    public KroxyliciousTesterBuilder addMockResponse(ResponsePayload responsePayload) {
        this.mockPayloads.add(responsePayload);
        return this;
    }

    public KroxyliciousTester createDefaultKroxyliciousTester() {
        final TrustStoreConfiguration trustStoreConfiguration = trustStoreLocation != null ? new TrustStoreConfiguration(trustStoreLocation, trustStorePassword) : null;
        return new DefaultKroxyliciousTester(configurationBuilder, kroxyliciousFactory, clientFactory, trustStoreConfiguration);
    }

    /**
     * Creates a kroxylicious tester for a kroxylicious instance that is proxying a
     * mock kafka broker. The mock can be configured to respond with an exact API
     * message, and be used to verify what was sent to it.
     * @return MockServerKroxyliciousTester
     */
    public MockServerKroxyliciousTester createMockKroxyliciousTester() {
        final MockServer mockServer = MockServer.startOnRandomPort();
        for (ResponsePayload mockPayload : mockPayloads) {
            mockServer.addMockResponseForApiKey(mockPayload);
        }
        return new MockServerKroxyliciousTester(mockServer, mockConfigurationFunction);
    }

    public KroxyliciousTester createTester() {
        if (mockPayloads.isEmpty()) {
            return createDefaultKroxyliciousTester();
        }
        else {
            return createMockKroxyliciousTester();
        }
    }

    public record TrustStoreConfiguration(String trustStoreLocation, @Nullable String trustStorePassword) {}
}
