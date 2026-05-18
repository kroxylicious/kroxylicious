/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouterChainFactoryTest {

    private static final List<RouteDefinition> DUMMY_ROUTES = List.of(
            new RouteDefinition("route1", 0, null, new RouteTarget("someCluster", null)));

    private static final String VC_NAME = "testVc";

    @Test
    void shouldHandleNullDefinitions() {
        try (var factory = new RouterChainFactory(testPfr(), List.of(), null)) {
            assertThatThrownBy(() -> factory.createRouter("nonexistent", VC_NAME))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldHandleEmptyDefinitions() {
        try (var factory = new RouterChainFactory(testPfr(), List.of(), List.of())) {
            assertThatThrownBy(() -> factory.createRouter("nonexistent", VC_NAME))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldCreateRouterInstance() {
        var rd = new RouterDefinition("myRouter", TestRouterFactory.class.getName(),
                null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "myRouter");
        try (var factory = new RouterChainFactory(testPfr(), List.of(vc), List.of(rd))) {
            Router router = factory.createRouter("myRouter", VC_NAME);
            assertThat(router).isNotNull();
        }
    }

    @Test
    void shouldThrowForUnknownRouterName() {
        var rd = new RouterDefinition("myRouter", TestRouterFactory.class.getName(),
                null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "myRouter");
        try (var factory = new RouterChainFactory(testPfr(), List.of(vc), List.of(rd))) {
            assertThatThrownBy(() -> factory.createRouter("unknown", VC_NAME))
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
        var vc = testVc(VC_NAME, "r1");
        try (var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rd))) {
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
        var routes1 = List.of(new RouteDefinition("r", 0, null, new RouteTarget(null, "r2")));
        var rd1 = new RouterDefinition("r1", "factory-0", null, routes1);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "r1");
        var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rd1, rd2));
        factory.close();

        assertThat(closeOrder).containsExactly("second", "first");
    }

    @Test
    void shouldThrowAfterClose() {
        var rd = new RouterDefinition("r1", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "r1");
        var factory = new RouterChainFactory(testPfr(), List.of(vc), List.of(rd));
        factory.close();

        assertThatThrownBy(() -> factory.createRouter("r1", VC_NAME))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldRejectMismatchedConfigType() {
        var pfr = testPfrWithConfigType(String.class);
        var rd = new RouterDefinition("r1", TestRouterFactory.class.getName(),
                Integer.valueOf(42), DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "r1");

        assertThatThrownBy(() -> new RouterChainFactory(pfr, List.of(vc), List.of(rd)))
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
        var routes1 = List.of(new RouteDefinition("r", 0, null, new RouteTarget(null, "r2")));
        var rd1 = new RouterDefinition("r1", "factory-0", null, routes1);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "r1");

        assertThatThrownBy(() -> new RouterChainFactory(pfr, List.of(vc), List.of(rd1, rd2)))
                .isInstanceOf(PluginConfigurationException.class);
        assertThat(closed.get()).isTrue();
    }

    @Test
    void shouldSurfaceExceptionFromCloseAndStillCloseOthers() {
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
        var routes1 = List.of(new RouteDefinition("r", 0, null, new RouteTarget(null, "r2")));
        var rd1 = new RouterDefinition("r1", "factory-0", null, routes1);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "r1");
        var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rd1, rd2));

        assertThatThrownBy(factory::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("close boom");
        assertThat(secondClosed.get()).isTrue();
    }

    @Test
    void shouldSuppressSecondCloseException() {
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
        var routes1 = List.of(new RouteDefinition("r", 0, null, new RouteTarget(null, "r2")));
        var rd1 = new RouterDefinition("r1", "factory-0", null, routes1);
        var rd2 = new RouterDefinition("r2", "factory-1", null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "r1");
        var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rd1, rd2));

        assertThatThrownBy(factory::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("second close boom")
                .satisfies(e -> assertThat(e.getSuppressed()).hasSize(1)
                        .singleElement().satisfies(s -> assertThat(s).hasMessage("first close boom")));
    }

    @Test
    void shouldPassCorrectContextToInitialize() {
        var capturedContext = new AtomicReference<RouterFactoryContext>();
        var pfr = testPfrWith(new TestRouterFactory() {
            @Override
            public Object initialize(RouterFactoryContext context, Object config) {
                capturedContext.set(context);
                return super.initialize(context, config);
            }
        });
        var rd = new RouterDefinition("myRouter", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var vc = testVc("myVc", "myRouter");
        try (var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rd))) {
            assertThat(capturedContext.get().virtualClusterName()).isEqualTo("myVc");
            assertThat(capturedContext.get().routerName()).isEqualTo("myRouter");
        }
    }

    @Test
    void shouldInitialiseSameRouterSeparatelyPerVirtualCluster() {
        var initCount = new AtomicInteger(0);
        var pfr = testPfrWith(new TestRouterFactory() {
            @Override
            public Object initialize(RouterFactoryContext context, Object config) {
                initCount.incrementAndGet();
                return new Object();
            }
        });
        var rd = new RouterDefinition("shared", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var vc1 = testVc("vc1", "shared");
        var vc2 = testVc("vc2", "shared");
        try (var factory = new RouterChainFactory(pfr, List.of(vc1, vc2), List.of(rd))) {
            assertThat(initCount.get()).isEqualTo(2);
            assertThat(factory.createRouter("shared", "vc1")).isNotNull();
            assertThat(factory.createRouter("shared", "vc2")).isNotNull();
        }
    }

    @Test
    void shouldNotInitialiseRoutersNotReferencedByAnyVc() {
        var initCount = new AtomicInteger(0);
        var pfr = testPfrWith(new TestRouterFactory() {
            @Override
            public Object initialize(RouterFactoryContext context, Object config) {
                initCount.incrementAndGet();
                return super.initialize(context, config);
            }
        });
        var rd1 = new RouterDefinition("used", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var rdOrphan = new RouterDefinition("orphan", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "used");
        try (var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rd1, rdOrphan))) {
            assertThat(initCount.get()).isEqualTo(1);
        }
    }

    @Test
    void shouldInitialiseSubRouters() {
        var initCount = new AtomicInteger(0);
        var pfr = testPfrWith(new TestRouterFactory() {
            @Override
            public Object initialize(RouterFactoryContext context, Object config) {
                initCount.incrementAndGet();
                return super.initialize(context, config);
            }
        });
        var routes1 = List.of(new RouteDefinition("toChild", 0, null, new RouteTarget(null, "child")));
        var rdParent = new RouterDefinition("parent", TestRouterFactory.class.getName(), null, routes1);
        var rdChild = new RouterDefinition("child", TestRouterFactory.class.getName(), null, DUMMY_ROUTES);
        var vc = testVc(VC_NAME, "parent");
        try (var factory = new RouterChainFactory(pfr, List.of(vc), List.of(rdParent, rdChild))) {
            assertThat(initCount.get()).isEqualTo(2);
            assertThat(factory.createRouter("parent", VC_NAME)).isNotNull();
            assertThat(factory.createRouter("child", VC_NAME)).isNotNull();
        }
    }

    // -- test helpers --

    static class TestRouterFactory implements RouterFactory<Object, Object> {

        @Override
        public Object initialize(RouterFactoryContext context, Object config) {
            return new Object();
        }

        @Override
        public Router createRouter(RouterFactoryContext context, Object initializationData) {
            return (apiKey, apiVersion, header, request, routingContext) -> CompletableFuture.completedFuture(null);
        }

        @SuppressWarnings("java:S1186") // intentional no-op for test factory
        @Override
        public void close(Object initializationData) {
        }
    }

    private static VirtualCluster testVc(String name, String router) {
        var gateway = new VirtualClusterGateway("gw",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9192), null, null, null),
                null, Optional.empty());
        return new VirtualCluster(name, null, new RouteTarget(null, router),
                List.of(gateway), false, false, null, null, null, null);
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
}
