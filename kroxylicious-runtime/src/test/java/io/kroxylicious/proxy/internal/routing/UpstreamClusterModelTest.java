/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TlsCredentialSupplierConfig;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.tls.TlsTestConstants;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class UpstreamClusterModelTest {

    private static final TargetCluster PLAINTEXT_CLUSTER = new TargetCluster("broker:9092", Optional.empty());
    private static final Tls TLS_NO_CREDENTIAL_SUPPLIER = new Tls(null, null, null, null, null);

    @Plugin(configType = Void.class)
    static class StubSupplierFactory implements ServerTlsCredentialSupplierFactory<Void, Void> {
        @Override
        public Void initialize(ServerTlsCredentialSupplierFactoryContext ctx, Void config) throws PluginConfigurationException {
            return null;
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext ctx, Void initData) {
            return mock(ServerTlsCredentialSupplier.class);
        }
    }

    private static PluginFactoryRegistry stubPfr() {
        return new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String name) {
                        return new StubSupplierFactory();
                    }

                    @Override
                    public Class<?> configType(String name) {
                        return Void.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("StubSupplierFactory");
                    }
                };
            }
        };
    }

    private static UpstreamClusterModel plaintext() {
        return new UpstreamClusterModel(PLAINTEXT_CLUSTER, Optional.empty(), TlsCredentialSupplierManager.unconfigured());
    }

    // tls()

    @Test
    void tlsReturnsEmptyWhenNoTlsConfigured() {
        assertThat(plaintext().tls()).isEmpty();
    }

    @Test
    void tlsReturnsTlsConfig() {
        var cluster = new TargetCluster("broker:9092", Optional.of(TLS_NO_CREDENTIAL_SUPPLIER));
        var model = new UpstreamClusterModel(cluster, Optional.empty(), TlsCredentialSupplierManager.unconfigured());
        assertThat(model.tls()).contains(TLS_NO_CREDENTIAL_SUPPLIER);
    }

    // bootstrapServersList()

    @Test
    void bootstrapServersListReturnsParsedList() {
        assertThat(plaintext().bootstrapServersList()).containsExactly(new io.kroxylicious.proxy.service.HostPort("broker", 9092));
    }

    @Test
    void bootstrapServersListReturnsAllServers() {
        var cluster = new TargetCluster("a:9092,b:9093", Optional.empty());
        var model = new UpstreamClusterModel(cluster, Optional.empty(), TlsCredentialSupplierManager.unconfigured());
        assertThat(model.bootstrapServersList()).hasSize(2);
    }

    // bootstrapServer()

    @Test
    void bootstrapServerReturnsSingleServer() {
        assertThat(plaintext().bootstrapServer()).isEqualTo(new io.kroxylicious.proxy.service.HostPort("broker", 9092));
    }

    // usesDynamicTlsCredentials()

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenNoTls() {
        assertThat(plaintext().usesDynamicTlsCredentials()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenTlsHasNoCredentialSupplier() {
        var cluster = new TargetCluster("broker:9092", Optional.of(TLS_NO_CREDENTIAL_SUPPLIER));
        var model = new UpstreamClusterModel(cluster, Optional.empty(), TlsCredentialSupplierManager.unconfigured());
        assertThat(model.usesDynamicTlsCredentials()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsTrueWhenCredentialSupplierConfigured() {
        var supplierConfig = new TlsCredentialSupplierConfig("StubSupplierFactory", null);
        var tls = new Tls(null, null, null, null, supplierConfig);
        var cluster = new TargetCluster("broker:9092", Optional.of(tls));
        var model = new UpstreamClusterModel(cluster, Optional.empty(), TlsCredentialSupplierManager.unconfigured());
        assertThat(model.usesDynamicTlsCredentials()).isTrue();
    }

    // build()

    @Test
    void buildCreatesModelWithSslContextAndUnconfiguredManagerForPlaintextCluster() {
        var model = UpstreamClusterModel.build(PLAINTEXT_CLUSTER, null);
        assertThat(model.upstreamSslContext()).isEmpty();
        assertThat(model.tlsManager().isConfigured()).isFalse();
    }

    @Test
    void buildCreatesModelWithConfiguredManagerWhenCredentialSupplierPresent() {
        var supplierConfig = new TlsCredentialSupplierConfig("StubSupplierFactory", null);
        var cluster = new TargetCluster("broker:9092", Optional.of(new Tls(null, null, null, null, supplierConfig)));

        var model = UpstreamClusterModel.build(cluster, stubPfr());

        assertThat(model.tlsManager().isConfigured()).isTrue();
        model.close();
    }

    @Test
    void buildThrowsWhenUpstreamTlsHasServerOptions() {
        var client = TlsTestConstants.getResourceLocationOnFilesystem("client.jks");
        var downstreamTls = new Tls(null,
                new TrustStore(client, new InlinePassword("storepass"), null, new ServerOptions(TlsClientAuth.REQUIRED)),
                null, null, null);
        var cluster = new TargetCluster("bootstrap:9092", Optional.of(downstreamTls));

        assertThatThrownBy(() -> UpstreamClusterModel.build(cluster, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Cannot apply trust options");
    }

    // close()

    @Test
    void closeClosesTheTlsManager() {
        var tlsManager = mock(TlsCredentialSupplierManager.class);
        var model = new UpstreamClusterModel(PLAINTEXT_CLUSTER, Optional.empty(), tlsManager);
        model.close();
        verify(tlsManager).close();
    }
}
