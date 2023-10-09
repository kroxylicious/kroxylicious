/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.util.function.Function;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KroxyliciousTesterBuilder {

    private String trustStoreLocation = null;
    private String trustStorePassword = null;
    private Function<Configuration, AutoCloseable> kroxyliciousFactory = DefaultKroxyliciousTester::spawnProxy;
    private DefaultKroxyliciousTester.ClientFactory clientFactory = (clusterName, defaultConfiguration) -> new KroxyliciousClients(defaultConfiguration);
    private ConfigurationBuilder configurationBuilder;

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

    public KroxyliciousTester createDefaultKroxyliciousTester() {
        final TrustStoreConfiguration trustStoreConfiguration = trustStoreLocation != null ? new TrustStoreConfiguration(trustStoreLocation, trustStorePassword) : null;
        return new DefaultKroxyliciousTester(configurationBuilder, kroxyliciousFactory, clientFactory, trustStoreConfiguration);
    }

    public record TrustStoreConfiguration(String trustStoreLocation, @Nullable String trustStorePassword) {}
}
