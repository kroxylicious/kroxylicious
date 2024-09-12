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

/**
 * Kroxylicious strives to offer high quality testing tools which allow filter authors to focus on their business logic.
 * However, as Kroxylicious is very configurable so is the Tester. Most of the time users do not care about a lot of the configuration options
 * and thus a builder, which can provide sensible defaults, provides the simplest API to configure things.
 */
public class KroxyliciousTesterBuilder {

    private String trustStoreLocation = null;
    @Nullable
    private String trustStorePassword = null;
    private Function<Configuration, AutoCloseable> kroxyliciousFactory = DefaultKroxyliciousTester::spawnProxy;
    private DefaultKroxyliciousTester.ClientFactory clientFactory = (clusterName, defaultConfiguration) -> new KroxyliciousClients(defaultConfiguration);
    private ConfigurationBuilder configurationBuilder;

    /**
     * Supplies the builder used to configure the kroxylicious instance to be used in tests.
     * @param configurationBuilder the builder instance ussd to configure the kroxylicious instance.
     * @return the current instance of the builder for chaining in a fluent fashion.
     */
    public KroxyliciousTesterBuilder setConfigurationBuilder(ConfigurationBuilder configurationBuilder) {
        this.configurationBuilder = configurationBuilder;
        return this;
    }

    /**
     * Configure the location of the trust store.
     * <p>
     * <strong>Required when using SSL/TLS</strong>
     * @param trustStoreLocation the path to the trust store to be used when clients connect to an SSL enabled Kroxylicious instance.
     * @return the current instance of the builder for chaining in a fluent fashion.
     */
    public KroxyliciousTesterBuilder setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
        return this;
    }

    /**
     * Configure the password of the trust store.
     * <p>
     * <strong>Optional when using SSL/TLS</strong>
     * @param trustStorePassword the password used to secure the trust store.
     * @return the current instance of the builder for chaining in a fluent fashion.
     */
    public KroxyliciousTesterBuilder setTrustStorePassword(@Nullable
    String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    /**
     * Explicitly creates a specific type of tester.
     * @return A {@link DefaultKroxyliciousTester} configured with the options specified by the builder.
     */
    public DefaultKroxyliciousTester createDefaultKroxyliciousTester() {
        final TrustStoreConfiguration trustStoreConfiguration = trustStoreLocation != null ? new TrustStoreConfiguration(trustStoreLocation, trustStorePassword) : null;
        return new DefaultKroxyliciousTester(configurationBuilder, kroxyliciousFactory, clientFactory, trustStoreConfiguration);
    }

    /**
     * Supplies a function which translates Kroxylicious config into a kroxylicious instance.
     * <p>
     * <em>Intended for internal use to allow unit testing of the testers.</em>
     *
     * @param kroxyliciousFactory config translation function.
     * @return the current instance of the builder for chaining in a fluent fashion.
     */
    KroxyliciousTesterBuilder setKroxyliciousFactory(Function<Configuration, AutoCloseable> kroxyliciousFactory) {
        this.kroxyliciousFactory = kroxyliciousFactory;
        return this;
    }

    /**
     * Supplies a custom factory for creating Kafka Clients.
     * <p>
     * <em>Intended for internal use to allow unit testing of the testers.</em>
     *
     * @param clientFactory a factory for creating Kafka Client instances.
     * @return the current instance of the builder for chaining in a fluent fashion.
     */
    KroxyliciousTesterBuilder setClientFactory(DefaultKroxyliciousTester.ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    record TrustStoreConfiguration(String trustStoreLocation, @Nullable
    String trustStorePassword) {
    }
}
