/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.router.RouterResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouterChainFactoryTest {

    private static final List<RouteDefinition> DUMMY_ROUTES = List.of(
            new RouteDefinition("route1", 0, null, new RouteTarget("someCluster", null)));

    @Test
    void shouldHandleNullDefinitions() {
        try (var factory = new RouterChainFactory(testPfr(), null)) {
            assertThatThrownBy(() -> factory.createRouter("nonexistent", testContext()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldHandleEmptyDefinitions() {
        try (var factory = new RouterChainFactory(testPfr(), List.of())) {
            assertThatThrownBy(() -> factory.createRouter("nonexistent", testContext()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldCreateRouterInstance() {
        var rd = new RouterDefinition("myRouter", TestRouterFactory.class.getName(),
                null, DUMMY_ROUTES);
        try (var factory = new RouterChainFactory(testPfr(), List.of(rd))) {
            Router router = factory.createRouter("myRouter", testContext());
            assertThat(router).isNotNull();
        }
    }

    @Test
    void shouldThrowForUnknownRouterName() {
        var rd = new RouterDefinition("myRouter", TestRouterFactory.class.getName(),
                null, DUMMY_ROUTES);
        try (var factory = new RouterChainFactory(testPfr(), List.of(rd))) {
            assertThatThrownBy(() -> factory.createRouter("unknown", testContext()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown");
        }
    }

    @Test
    void shouldInitializeFactoryOnConstruction() {
        var initCount = new AtomicInteger(0);
        var pfr = testPfrWith(new TestRouterFactory() {
            @Override
            public Object initialize(RouterFactoryContext context, Object config) {
                initCount.incrementAndGet();
                return super.initialize(context, config);
            }
        });
        var rd = new RouterDefinition("r1", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        try (var factory = new RouterChainFactory(pfr, List.of(rd))) {
            assertThat(initCount.get()).isEqualTo(1);
        }
    }

    @Test
    void shouldCloseFactoriesInReverseOrder() {
        var closeOrder = new java.util.ArrayList<String>();
        var pfr = testPfrWithMultiple(
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        closeOrder.add("first");
                    }
                },
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        closeOrder.add("second");
                    }
                });
        var rd1 = new RouterDefinition("r1", "factory-0", null, DUMMY_ROUTES);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var factory = new RouterChainFactory(pfr, List.of(rd1, rd2));
        factory.close();

        assertThat(closeOrder).containsExactly("second", "first");
    }

    @Test
    void shouldThrowAfterClose() {
        var rd = new RouterDefinition("r1", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var factory = new RouterChainFactory(testPfr(), List.of(rd));
        factory.close();

        assertThatThrownBy(() -> factory.createRouter("r1", testContext()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldRejectMismatchedConfigType() {
        var pfr = testPfrWithConfigType(String.class);
        var rd = new RouterDefinition("r1", TestRouterFactory.class.getName(),
                Integer.valueOf(42), DUMMY_ROUTES);

        assertThatThrownBy(() -> new RouterChainFactory(pfr, List.of(rd)))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("config of type");
    }

    @Test
    void shouldCloseAlreadyInitializedOnFailure() {
        var closed = new AtomicBoolean(false);
        var factories = new TestRouterFactory[]{
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        closed.set(true);
                    }
                },
                new TestRouterFactory() {
                    @Override
                    public Object initialize(RouterFactoryContext context, Object config) {
                        throw new RuntimeException("init boom");
                    }
                }
        };
        var pfr = testPfrWithMultiple(factories);
        var rd1 = new RouterDefinition("r1", "factory-0", null, DUMMY_ROUTES);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);

        assertThatThrownBy(() -> new RouterChainFactory(pfr, List.of(rd1, rd2)))
                .isInstanceOf(PluginConfigurationException.class);
        assertThat(closed.get()).isTrue();
    }

    // -- test helpers --

    static class TestRouterFactory implements RouterFactory<Object, Object> {
        @Override
        public Object initialize(RouterFactoryContext context, Object config) {
            return new Object();
        }

        @Override
        public Router createRouter(RouterFactoryContext context, Object initializationData) {
            return (apiVersion, apiKey, header, request, routerContext) -> CompletableFuture.completedFuture(new RouterResult.CompletedNoResponse());
        }

        @Override
        public void close(Object initializationData) {
        }
    }

    private static PluginFactoryRegistry testPfr() {
        return testPfrWith(new TestRouterFactory());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static PluginFactoryRegistry testPfrWith(TestRouterFactory factoryInstance) {
        return new PluginFactoryRegistry() {
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return (PluginFactory<P>) new PluginFactory<RouterFactory>() {
                    @Override
                    public RouterFactory pluginInstance(String instanceName) {
                        return factoryInstance;
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return Object.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of(instanceName(factoryInstance));
                    }

                    private String instanceName(TestRouterFactory f) {
                        return f.getClass().getName();
                    }
                };
            }
        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static PluginFactoryRegistry testPfrWithMultiple(TestRouterFactory... factories) {
        return new PluginFactoryRegistry() {
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return (PluginFactory<P>) new PluginFactory<RouterFactory>() {
                    @Override
                    public RouterFactory pluginInstance(String instanceName) {
                        if (instanceName.startsWith("factory-")) {
                            int idx = Integer.parseInt(instanceName.substring("factory-".length()));
                            return factories[idx];
                        }
                        for (var f : factories) {
                            if (instanceName.equals(f.getClass().getName())) {
                                return f;
                            }
                        }
                        throw new RuntimeException("Unknown factory: " + instanceName);
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return Object.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of();
                    }
                };
            }
        };
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static PluginFactoryRegistry testPfrWithConfigType(Class<?> expectedConfigType) {
        return new PluginFactoryRegistry() {
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return (PluginFactory<P>) new PluginFactory<RouterFactory>() {
                    @Override
                    public RouterFactory pluginInstance(String instanceName) {
                        return new TestRouterFactory();
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return expectedConfigType;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of();
                    }
                };
            }
        };
    }

    private static RouterFactoryContext testContext() {
        return new RouterFactoryContext() {
            @Override
            public String virtualClusterName() {
                return "test-cluster";
            }

            @Override
            public String routerName() {
                return "test-router";
            }

            @Override
            public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                return Set.of();
            }
        };
    }
}
