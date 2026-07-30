/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.bootstrap.TlsCredentialSupplierManager;
import io.kroxylicious.proxy.config.CacheConfiguration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsCredentialSupplierConfig;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.internal.filter.FlakyConfig;
import io.kroxylicious.proxy.internal.filter.FlakyFactory;
import io.kroxylicious.proxy.internal.routing.DirectRouting;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.NoUpstreamClusterForRouteException;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.internal.routing.UpstreamClusterModel;
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

    public static final String DIRECT_ROUTE_NAME = "upstream";

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

    private static final List<NamedFilterDefinition> EMPTY_FILTERS = List.of();

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

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(DIRECT_ROUTE_NAME, targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
        assertThat(model.getUpstreamClusterForRoute(DIRECT_ROUTE_NAME).tlsManager().isConfigured()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsFalseWhenTlsHasNoCredentialSupplier() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, null)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(DIRECT_ROUTE_NAME, targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isFalse();
    }

    @Test
    void usesDynamicTlsCredentialsReturnsTrueWhenCredentialSupplierConfigured() {
        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(DIRECT_ROUTE_NAME, targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.usesDynamicTlsCredentials()).isTrue();
    }

    @Test
    void tlsCredentialSupplierManagerIsAccessibleViaRoute() {
        var credentialSupplierConfig = new TlsCredentialSupplierConfig("TestSupplierFactory", new TestSupplierConfig("test"));
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.of(new Tls(null, null, null, null, credentialSupplierConfig)));
        var clusterModel = UpstreamClusterModel.build(targetCluster, pluginFactoryRegistry());

        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(DIRECT_ROUTE_NAME, clusterModel), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);

        assertThat(model.getUpstreamClusterForRoute(DIRECT_ROUTE_NAME).tlsManager().isConfigured()).isTrue();
        assertThat(model.getUpstreamClusterForRoute(DIRECT_ROUTE_NAME).tlsManager().getSupplier()).isNotNull();
        model.close();
    }

    @Test
    void closeIsNoOpWhenTlsCredentialSupplierManagerIsUnconfigured() {
        TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());
        VirtualClusterModel model = new VirtualClusterModel("wibble", new DirectRouting(DIRECT_ROUTE_NAME, targetCluster), false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), null);
        assertThatCode(model::close).doesNotThrowAnyException();
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
        var clusterModel = UpstreamClusterModel.build(targetCluster, combinedPluginFactoryRegistry());
        var model = new VirtualClusterModel("vc1", new DirectRouting(DIRECT_ROUTE_NAME, clusterModel), false, false, filters,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), combinedPluginFactoryRegistry());

        TlsCredentialSupplierManager tlsManager = clusterModel.tlsManager();
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

        DirectRouting directRouting = new DirectRouting(DIRECT_ROUTE_NAME, targetCluster);
        assertThatThrownBy(() -> new VirtualClusterModel("vc1", directRouting, false, false, filters,
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
        assertThatThrownBy(() -> model.getUpstreamClusterForRoute("route1")).isInstanceOf(NoUpstreamClusterForRouteException.class);
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
    void createRouteFiltersShouldReturnDistinctInstancesWhenTwoRoutesShareSameFilterName() {
        // Given
        var sharedFilterName = "flaky";
        var flakyConfig = new FlakyConfig(null, null, null, c -> {
        }, c -> {
        });
        var filterDef = new NamedFilterDefinition(sharedFilterName, FlakyFactory.class.getName(), flakyConfig);
        var routeA = new RouteDescriptor("route-a", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of(filterDef));
        var routeB = new RouteDescriptor("route-b", 1, new TargetCluster("broker:9093", Optional.empty()), null, List.of(filterDef));
        var model = new VirtualClusterModel("vc",
                new DynamicRouting("myrouter", Map.of("route-a", routeA, "route-b", routeB), mock(RouterChainFactory.class)),
                false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), combinedPluginFactoryRegistry());
        var filterContext = mock(io.kroxylicious.proxy.filter.FilterFactoryContext.class);

        // When
        var filtersA = model.createRouteFilters("route-a", filterContext);
        var filtersB = model.createRouteFilters("route-b", filterContext);

        // Then
        assertThat(filtersA).hasSize(1);
        assertThat(filtersB).hasSize(1);
        assertThat(filtersA.get(0).filter()).isNotSameAs(filtersB.get(0).filter());
    }

    @Test
    void createRouteFiltersShouldReturnEmptyListForRouteWithNoFilters() {
        // Given
        var route = new RouteDescriptor("route-a", 0, new TargetCluster("broker:9092", Optional.empty()), null, List.of());
        var model = new VirtualClusterModel("vc",
                new DynamicRouting("myrouter", Map.of("route-a", route), mock(RouterChainFactory.class)),
                false, false, EMPTY_FILTERS,
                CacheConfiguration.DEFAULT, null, Duration.ofSeconds(10), combinedPluginFactoryRegistry());
        var filterContext = mock(io.kroxylicious.proxy.filter.FilterFactoryContext.class);

        // When
        var filters = model.createRouteFilters("route-a", filterContext);

        // Then
        assertThat(filters).isEmpty();
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

    // generateTlsSummary()

    @Test
    void generateTlsSummaryReturnsEmptyStringForNoTls() {
        assertThat(VirtualClusterModel.generateTlsSummary(Optional.empty())).isEmpty();
    }

    @Test
    void generateTlsSummaryIncludesTlsMarkerWhenTlsPresent() {
        var summary = VirtualClusterModel.generateTlsSummary(Optional.of(new Tls(null, null, null, null, null)));
        assertThat(summary).contains("(TLS:");
    }

    @Test
    void generateTlsSummaryIncludesAllowedCipherSuites() {
        var tls = new Tls(null, null, new AllowDeny<>(List.of("TLS_AES_256_GCM_SHA384"), null), null, null);
        var summary = VirtualClusterModel.generateTlsSummary(Optional.of(tls));
        assertThat(summary).contains("Allowed Ciphers").contains("TLS_AES_256_GCM_SHA384");
    }

    @Test
    void generateTlsSummaryIncludesDeniedCipherSuites() {
        var tls = new Tls(null, null, new AllowDeny<>(null, Set.of("TLS_RSA_WITH_NULL_MD5")), null, null);
        var summary = VirtualClusterModel.generateTlsSummary(Optional.of(tls));
        assertThat(summary).contains("Denied Ciphers").contains("TLS_RSA_WITH_NULL_MD5");
    }

    @Test
    void generateTlsSummaryIncludesAllowedProtocols() {
        var tls = new Tls(null, null, null, new AllowDeny<>(List.of("TLSv1.3"), null), null);
        var summary = VirtualClusterModel.generateTlsSummary(Optional.of(tls));
        assertThat(summary).contains("Allowed Protocols").contains("TLSv1.3");
    }

    @Test
    void generateTlsSummaryIncludesDeniedProtocols() {
        var tls = new Tls(null, null, null, new AllowDeny<>(null, Set.of("TLSv1.1")), null);
        var summary = VirtualClusterModel.generateTlsSummary(Optional.of(tls));
        assertThat(summary).contains("Denied Protocols").contains("TLSv1.1");
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
