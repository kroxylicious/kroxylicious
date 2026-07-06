/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
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
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.FlakyConfig;
import io.kroxylicious.proxy.internal.filter.FlakyFactory;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.internal.tls.TlsTestConstants;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
    void usesDynamicTlsCredentialsReturnsFalseWhenDynamicRouting() {
        var routeDescriptors = Map.of("r1",
                new RouteDescriptor("r1", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of()));
        VirtualClusterModel model = new VirtualClusterModel("wibble",
                new DynamicRouting("some-router", routeDescriptors, mock(RouterChainFactory.class)),
                false, false, EMPTY_FILTERS, CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenNoTlsConfigured() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
        assertThat(model.getTlsCredentialSupplierManager().isConfigured()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenTlsHasNoCredentialSupplier() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, null)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsTrueWhenCredentialSupplierConfigured() {
        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isTrue();
    }

    @Test
    void initializesTlsCredentialSupplierManagerWhenPluginRegistryProvided() {
        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), pluginFactoryRegistry());

        assertThat(model.getTlsCredentialSupplierManager().isConfigured()).isTrue();
        assertThat(model.getTlsCredentialSupplierManager().getSupplier()).isNotNull();
        model.close();
    }

    @Test
    void closeIsNoOpWhenTlsCredentialSupplierManagerIsUnconfigured() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());
        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        model.close();
    }

    @Test
    void closeClosesBothFilterChainFactoryAndTlsCredentialSupplierManager() {
        // The merged close at VCM.close() must invoke close on both the FilterChainFactory and the
        // TlsCredentialSupplierManager. Observed via FlakyConfig.onClose for the filter leg and via
        // TlsCredentialSupplierManager.getSupplier throwing IllegalStateException post-close for the TLS leg.
        var onFilterClose = new AtomicInteger();
        var flakyConfig = new FlakyConfig(null, null, null, c -> {
        }, c -> onFilterClose.incrementAndGet());
        var filters = List.<NamedFilterDefinition> of(new NamedFilterDefinition("flaky", FlakyFactory.class.getName(), flakyConfig));

        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        var targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));
        var model = new VirtualClusterModel("vc1", new DirectRouting(targetCluster), false, false, filters,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), combinedPluginFactoryRegistry());

        var tlsManager = model.getTlsCredentialSupplierManager();
        assertThat(tlsManager.isConfigured()).isTrue();
        assertThat(tlsManager.getSupplier()).isNotNull();
        assertThat(onFilterClose.get()).isZero();

        model.close();

        assertThat(onFilterClose.get()).as("FilterChainFactory close should fire").isEqualTo(1);
        assertThatThrownBy(tlsManager::getSupplier)
                .as("TlsCredentialSupplierManager close should fire — post-close getSupplier throws")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void constructorPropagatesFilterInitializeFailureAsPluginConfigurationException() {
        // The contract that KafkaProxy.defaultRegistry's try-catch depends on: any filter init failure
        // during VCM construction surfaces as PluginConfigurationException, which defaultRegistry then
        // wraps as LifecycleException. If this exception type drifts, the wrap silently stops working
        // and startup error reporting regresses (the AuthzFailsClosedIT path).
        var flakyConfig = new FlakyConfig("init kaboom", null, null, c -> {
        }, c -> {
        });
        var filters = List.<NamedFilterDefinition> of(new NamedFilterDefinition("bad-filter", FlakyFactory.class.getName(), flakyConfig));
        var targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());
        var pfr = combinedPluginFactoryRegistry();
        var drainTimeout = Duration.ofSeconds(10);

        assertThatThrownBy(() -> new VirtualClusterModel("vc1", new DirectRouting(targetCluster), false, false, filters,
                CacheConfiguration.DEFAULT, null, drainTimeout, pfr))
                .isExactlyInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("Exception initializing filter factory bad-filter")
                .cause()
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("init kaboom");
    }

    @Test
    void dynamicRoutingVcmHasNoUpstreamSslContext() {
        var routeDescriptors = Map.of("route1",
                new RouteDescriptor("route1", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of()));
        var dynamicRouting = new DynamicRouting("myrouter", routeDescriptors, mock(RouterChainFactory.class));

        VirtualClusterModel model = new VirtualClusterModel("routed-vc", dynamicRouting, false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.routing()).isInstanceOf(DynamicRouting.class);
        assertThat(((DynamicRouting) model.routing()).routerName()).isEqualTo("myrouter");
        assertThat(((DynamicRouting) model.routing()).routeDescriptors()).containsKey("route1");
        assertThat(model.getUpstreamSslContext()).isEmpty();
    }

    @Test
    void logVirtualClusterSummaryWorksForDynamicRouting() {
        var routeDescriptors = Map.of("route1",
                new RouteDescriptor("route1", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of()));
        VirtualClusterModel model = new VirtualClusterModel("routed-vc",
                new DynamicRouting("myrouter", routeDescriptors, mock(RouterChainFactory.class)),
                false, false, EMPTY_FILTERS, CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThatCode(model::logVirtualClusterSummary).doesNotThrowAnyException();
    }

    @Test
    void shouldNotAllowUpstreamToProvideTlsServerOptions() {
        // Given
        final Optional<Tls> downstreamTls = Optional
                .of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, new ServerOptions(TlsClientAuth.REQUIRED)), null, null, null));
        final TargetCluster targetCluster = new TargetCluster("bootstrap:9092", downstreamTls);

        // When/Then
        assertThatThrownBy(() -> new VirtualClusterModel("wibble", new DirectRouting(targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Cannot apply trust options");
    }

    @Test
    void closeClosesRouterChainFactory() {
        // Given
        var rcf = mock(RouterChainFactory.class);
        var routeDescriptor = new RouteDescriptor("r1", 0, null, "nested", List.of());
        var model = new VirtualClusterModel("routed-vc",
                new DynamicRouting("r1", Map.of("r1", routeDescriptor), rcf),
                false, false, EMPTY_FILTERS, CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        // When
        model.close();

        // Then: the RouterChainFactory is closed
        verify(rcf).close();
    }

    @Test
    void closeStillClosesFilterChainFactoryWhenRouterChainFactoryCloseThrows() {
        // Given: a RouterChainFactory that throws on close
        var rcf = mock(RouterChainFactory.class);
        doThrow(new RuntimeException("rcf close boom")).when(rcf).close();
        var onFilterClose = new AtomicInteger();
        var flakyConfig = new FlakyConfig(null, null, null, c -> {
        }, c -> onFilterClose.incrementAndGet());
        var filters = List.<NamedFilterDefinition> of(new NamedFilterDefinition("flaky", FlakyFactory.class.getName(), flakyConfig));
        var routeDescriptor = new RouteDescriptor("r1", 0, new TargetCluster("bootstrap:9092", Optional.empty()), null, List.of());
        var model = new VirtualClusterModel("vc",
                new DynamicRouting("r1", Map.of("r1", routeDescriptor), rcf),
                false, false, filters,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), combinedPluginFactoryRegistry());

        // When
        assertThatThrownBy(model::close).hasMessage("rcf close boom");

        // Then: FilterChainFactory was still closed despite the exception
        assertThat(onFilterClose.get()).as("FilterChainFactory close should fire despite RCF failure").isEqualTo(1);
    }

    /**
     * PFR that dispatches on plugin class — FilterFactory routes to FlakyFactory; everything else
     * (in practice, ServerTlsCredentialSupplierFactory) routes to TestSupplierFactory. Used by tests
     * that exercise both filter and TLS legs simultaneously.
     */
    private static PluginFactoryRegistry combinedPluginFactoryRegistry() {
        return new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                if (pluginClass == FilterFactory.class) {
                    return new PluginFactory() {
                        @Override
                        public Object pluginInstance(String instanceName) {
                            return new FlakyFactory();
                        }

                        @Override
                        public Class<?> configType(String instanceName) {
                            return FlakyConfig.class;
                        }

                        @Override
                        public Set<String> registeredInstanceNames() {
                            return Set.of(FlakyFactory.class.getSimpleName());
                        }
                    };
                }
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
