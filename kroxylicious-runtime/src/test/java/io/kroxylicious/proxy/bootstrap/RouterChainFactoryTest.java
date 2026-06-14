/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterContext;
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
            var ctx = testContext();
            assertThatThrownBy(() -> factory.createRouter("nonexistent", ctx))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldHandleEmptyDefinitions() {
        try (var factory = new RouterChainFactory(testPfr(), List.of())) {
            var ctx = testContext();
            assertThatThrownBy(() -> factory.createRouter("nonexistent", ctx))
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
            var ctx = testContext();
            assertThatThrownBy(() -> factory.createRouter("unknown", ctx))
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

        var ctx = testContext();
        assertThatThrownBy(() -> factory.createRouter("r1", ctx))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldRejectMismatchedConfigType() {
        var pfr = testPfrWithConfigType(String.class);
        var rd = new RouterDefinition("r1", TestRouterFactory.class.getName(),
                Integer.valueOf(42), DUMMY_ROUTES);

        var defs = List.of(rd);
        assertThatThrownBy(() -> new RouterChainFactory(pfr, defs))
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

        var defs = List.of(rd1, rd2);
        assertThatThrownBy(() -> new RouterChainFactory(pfr, defs))
                .isInstanceOf(PluginConfigurationException.class);
        assertThat(closed.get()).isTrue();
    }

    @Test
    void shouldSurfaceExceptionFromCloseAndStillCloseOthers() {
        // Given
        var secondClosed = new AtomicBoolean(false);
        var pfr = testPfrWithMultiple(
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        secondClosed.set(true);
                    }
                },
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        throw new RuntimeException("close boom");
                    }
                });
        var rd1 = new RouterDefinition("r1", "factory-0", null, DUMMY_ROUTES);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var factory = new RouterChainFactory(pfr, List.of(rd1, rd2));

        // When / Then
        assertThatThrownBy(factory::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("close boom");
        assertThat(secondClosed.get()).isTrue();
    }

    @Test
    void shouldSuppressSecondCloseException() {
        // Given
        var pfr = testPfrWithMultiple(
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        throw new RuntimeException("first close boom");
                    }
                },
                new TestRouterFactory() {
                    @Override
                    public void close(Object initializationData) {
                        throw new RuntimeException("second close boom");
                    }
                });
        var rd1 = new RouterDefinition("r1", "factory-0", null, DUMMY_ROUTES);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var factory = new RouterChainFactory(pfr, List.of(rd1, rd2));

        // When / Then
        assertThatThrownBy(factory::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("second close boom")
                .satisfies(e -> assertThat(e.getSuppressed()).hasSize(1)
                        .singleElement().satisfies(s -> assertThat(s).hasMessage("first close boom")));
    }

    // -- test helpers --

    static class TestRouterFactory implements RouterFactory<Object, Object> {

        private static final Map<ApiKeys, String> ALL_STATIC = Arrays.stream(ApiKeys.values())
                .collect(Collectors.toUnmodifiableMap(k -> k, k -> "default"));

        @Override
        public Object initialize(RouterFactoryContext context, Object config) {
            return new Object();
        }

        @Override
        public Router createRouter(RouterFactoryContext context, Object initializationData) {
            return new Router() {
                @Override
                public CompletionStage<RouterResult> onRequest(short apiVersion, ApiKeys apiKey,
                                                               RequestHeaderData header, ApiMessage request,
                                                               RouterContext routerContext) {
                    throw new IllegalStateException("Dynamic routing is not supported");
                }

                @Override
                public Map<ApiKeys, String> staticRoutes() {
                    return ALL_STATIC;
                }
            };
        }

        @SuppressWarnings("java:S1186") // intentional no-op for test factory
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
