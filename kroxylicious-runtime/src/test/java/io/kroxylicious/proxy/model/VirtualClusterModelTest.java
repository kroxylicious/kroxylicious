/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TlsCredentialSupplierConfig;
import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class VirtualClusterModelTest {

    record TestSupplierConfig(String value) {}

    @Plugin(configType = TestSupplierConfig.class)
    public static class TestSupplierFactory implements ServerTlsCredentialSupplierFactory<TestSupplierConfig, TestSupplierConfig> {

        @Override
        public TestSupplierConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestSupplierConfig config) throws PluginConfigurationException {
            return config;
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestSupplierConfig initializationData) {
            return mock(ServerTlsCredentialSupplier.class);
        }
    }

    private static final InlinePassword PASSWORD_PROVIDER = new InlinePassword("storepass");

    private static final String KNOWN_CIPHER_SUITE;
    private static final List<NamedFilterDefinition> EMPTY_FILTERS = List.of();

    static {
        try {
            var defaultSSLParameters = SSLContext.getDefault().getDefaultSSLParameters();
            KNOWN_CIPHER_SUITE = defaultSSLParameters.getCipherSuites()[0];
            assertThat(KNOWN_CIPHER_SUITE).isNotNull();
            assertThat(defaultSSLParameters.getProtocols()).contains("TLSv1.2", "TLSv1.3");
        }
        catch (NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private String client;
    private KeyPair keyPair;

    @BeforeEach
    void setUp() {
        String privateKeyFile = TlsTestConstants.getResourceLocationOnFilesystem("server.key");
        String cert = TlsTestConstants.getResourceLocationOnFilesystem("server.crt");
        client = TlsTestConstants.getResourceLocationOnFilesystem("client.jks");
        keyPair = new KeyPair(privateKeyFile, cert, null);
    }

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenNoTlsConfigured() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());

        VirtualClusterModel model = new VirtualClusterModel("wibble", targetCluster, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
        assertThat(model.getTlsCredentialSupplierManager().isConfigured()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenTlsHasNoCredentialSupplier() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, null)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", targetCluster, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsTrueWhenCredentialSupplierConfigured() {
        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", targetCluster, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));

        assertThat(model.usesDynamicTlsCredentials()).isTrue();
    }

    @Test
    void initializesTlsCredentialSupplierManagerWhenPluginRegistryProvided() {
        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", targetCluster, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), pluginFactoryRegistry());

        assertThat(model.getTlsCredentialSupplierManager().isConfigured()).isTrue();
        assertThat(model.getTlsCredentialSupplierManager().getSupplier()).isNotNull();
        model.close();
    }

    @Test
    void closeIsNoOpWhenTlsCredentialSupplierManagerIsUnconfigured() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());
        VirtualClusterModel model = new VirtualClusterModel("wibble", targetCluster, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10));

        model.close();
    }

    @Test
    void shouldNotAllowUpstreamToProvideTlsServerOptions() {
        // Given
        final Optional<Tls> downstreamTls = Optional
                .of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, new ServerOptions(TlsClientAuth.REQUIRED)), null, null, null));
        final TargetCluster targetCluster = new TargetCluster("bootstrap:9092", downstreamTls);

        // When/Then
        assertThatThrownBy(() -> new VirtualClusterModel("wibble", targetCluster, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10)))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Cannot apply trust options");
    }

    private static PluginFactoryRegistry pluginFactoryRegistry() {
        return new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String instanceName) {
                        return new TestSupplierFactory();
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return TestSupplierConfig.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("TestSupplierFactory");
                    }
                };
            }
        };
    }

}
