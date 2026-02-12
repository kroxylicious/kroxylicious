/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.tls.TlsCredentialSupplierDefinition;
import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplier;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactoryContext;
import io.kroxylicious.proxy.tls.TlsCredentials;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class TlsCredentialSupplierManagerTest {

    private PluginFactoryRegistry pfr;
    private ServerTlsCredentialSupplierFactoryContext creationContext;

    // Test configuration classes
    public record TestConfig(String value) {}

    public record InvalidConfig(String value) {}

    // Test factory implementations
    @Plugin(configType = TestConfig.class)
    public static class TestSupplierFactory implements ServerTlsCredentialSupplierFactory<TestConfig, TestConfig> {

        private static int initializeCallCount = 0;
        private static int createCallCount = 0;
        private static int closeCallCount = 0;

        public static void resetCounters() {
            initializeCallCount = 0;
            createCallCount = 0;
            closeCallCount = 0;
        }

        @Override
        public TestConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config)
                throws PluginConfigurationException {
            initializeCallCount++;
            TestConfig validated = Plugins.requireConfig(this, config);
            if (validated.value().equals("invalid")) {
                throw new PluginConfigurationException("Invalid configuration");
            }
            return validated;
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestConfig initializationData) {
            createCallCount++;
            return mock(ServerTlsCredentialSupplier.class);
        }

        @Override
        public void close(TestConfig initializationData) {
            closeCallCount++;
        }
    }

    @Plugin(configType = Void.class)
    public static class NoConfigSupplierFactory implements ServerTlsCredentialSupplierFactory<Void, Void> {

        @Override
        public Void initialize(ServerTlsCredentialSupplierFactoryContext context, Void config) {
            return null;
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, Void initializationData) {
            return mock(ServerTlsCredentialSupplier.class);
        }
    }

    @Plugin(configType = TestConfig.class)
    public static class FailingInitializeFactory implements ServerTlsCredentialSupplierFactory<TestConfig, TestConfig> {

        @Override
        public TestConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config)
                throws PluginConfigurationException {
            throw new PluginConfigurationException("Initialization failed");
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestConfig initializationData) {
            throw new IllegalStateException("Should not be called");
        }
    }

    @Plugin(configType = TestConfig.class)
    public static class FailingCreateFactory implements ServerTlsCredentialSupplierFactory<TestConfig, TestConfig> {

        @Override
        public TestConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config) {
            return config;
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestConfig initializationData) {
            throw new RuntimeException("Creation failed");
        }
    }

    @BeforeEach
    void setUp() {
        TestSupplierFactory.resetCounters();

        pfr = new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String instanceName) {
                        return switch (instanceName) {
                            case "TestSupplierFactory" -> new TestSupplierFactory();
                            case "NoConfigSupplierFactory" -> new NoConfigSupplierFactory();
                            case "FailingInitializeFactory" -> new FailingInitializeFactory();
                            case "FailingCreateFactory" -> new FailingCreateFactory();
                            default -> throw new RuntimeException("Unknown factory: " + instanceName);
                        };
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return switch (instanceName) {
                            case "NoConfigSupplierFactory" -> Void.class;
                            case "TestSupplierFactory", "FailingInitializeFactory", "FailingCreateFactory" -> TestConfig.class;
                            default -> throw new RuntimeException("Unknown factory: " + instanceName);
                        };
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("TestSupplierFactory", "NoConfigSupplierFactory", "FailingInitializeFactory", "FailingCreateFactory");
                    }
                };
            }
        };

        creationContext = new ServerTlsCredentialSupplierFactoryContext() {
            @Override
            public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                return pfr.pluginFactory(pluginClass).pluginInstance(implementationName);
            }

            @Override
            public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                return pfr.pluginFactory(pluginClass).registeredInstanceNames();
            }

            @Override
            @NonNull
            public FilterDispatchExecutor filterDispatchExecutor() {
                return mock(FilterDispatchExecutor.class);
            }

            @Override
            @NonNull
            public TlsCredentials tlsCredentials(@NonNull java.security.PrivateKey key, @NonNull java.security.cert.Certificate[] certificateChain) {
                return mock(TlsCredentials.class);
            }
        };
    }

    @Test
    void testNullDefinitionCreatesNoFactory() {
        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, null)) {
            assertThat(manager.isConfigured()).isFalse();
            assertThat(manager.createSupplier(creationContext)).isNull();
        }
    }

    @Test
    void testFactoryInitializationWithConfig() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            assertThat(manager.isConfigured()).isTrue();
            assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.createCallCount).isEqualTo(0);
            assertThat(TestSupplierFactory.closeCallCount).isEqualTo(0);
        }

        // Verify close was called
        assertThat(TestSupplierFactory.closeCallCount).isEqualTo(1);
    }

    @Test
    void testFactoryInitializationWithoutConfig() {
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("NoConfigSupplierFactory", null);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            assertThat(manager.isConfigured()).isTrue();
            ServerTlsCredentialSupplier supplier = manager.createSupplier(creationContext);
            assertThat(supplier).isNotNull();
        }
    }

    @Test
    void testCreateSupplierMultipleTimes() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            ServerTlsCredentialSupplier supplier1 = manager.createSupplier(creationContext);
            ServerTlsCredentialSupplier supplier2 = manager.createSupplier(creationContext);
            ServerTlsCredentialSupplier supplier3 = manager.createSupplier(creationContext);

            assertThat(supplier1).isNotNull();
            assertThat(supplier2).isNotNull();
            assertThat(supplier3).isNotNull();

            // Initialize called once, create called three times
            assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.createCallCount).isEqualTo(3);
        }
    }

    @Test
    void testInvalidConfigurationThrowsException() {
        TestConfig config = new TestConfig("invalid");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        assertThatThrownBy(() -> new TlsCredentialSupplierManager(pfr, definition))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("Invalid configuration")
                .hasMessageContaining("TestSupplierFactory");
    }

    @Test
    void testInitializationFailureThrowsException() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("FailingInitializeFactory", config);

        assertThatThrownBy(() -> new TlsCredentialSupplierManager(pfr, definition))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("Initialization failed")
                .hasMessageContaining("FailingInitializeFactory");
    }

    @Test
    void testCreationFailureThrowsException() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("FailingCreateFactory", config);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            assertThatThrownBy(() -> manager.createSupplier(creationContext))
                    .isInstanceOf(PluginConfigurationException.class)
                    .hasMessageContaining("FailingCreateFactory")
                    .cause()
                    .hasMessageContaining("Creation failed");
        }
    }

    @Test
    void testWrongConfigTypeThrowsException() {
        InvalidConfig config = new InvalidConfig("wrong-type");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        assertThatThrownBy(() -> new TlsCredentialSupplierManager(pfr, definition))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("accepts config of type")
                .hasMessageContaining(TestConfig.class.getName())
                .hasMessageContaining(InvalidConfig.class.getName());
    }

    @Test
    void testUnknownFactoryTypeThrowsException() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("UnknownFactory", config);

        assertThatThrownBy(() -> new TlsCredentialSupplierManager(pfr, definition))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unknown factory: UnknownFactory");
    }

    @Test
    void testCreateSupplierAfterCloseThrowsException() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition);
        manager.close();

        assertThatThrownBy(() -> manager.createSupplier(creationContext))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is closed");
    }

    @Test
    void testPluginInstanceMethodIsAvailable() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        // Create a factory that tests pluginInstance during initialization
        @Plugin(configType = TestConfig.class)
        class PluginInstanceTestFactory implements ServerTlsCredentialSupplierFactory<TestConfig, TestConfig> {
            private boolean pluginInstanceCalled = false;

            @Override
            public TestConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config) {
                // Test that we can call pluginInstance
                Set<String> names = context.pluginImplementationNames(ServerTlsCredentialSupplierFactory.class);
                assertThat(names).contains("TestSupplierFactory");
                pluginInstanceCalled = true;
                return config;
            }

            @Override
            public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestConfig initializationData) {
                return mock(ServerTlsCredentialSupplier.class);
            }

            public boolean wasPluginInstanceCalled() {
                return pluginInstanceCalled;
            }
        }

        PluginInstanceTestFactory testFactory = new PluginInstanceTestFactory();

        PluginFactoryRegistry testPfr = new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String instanceName) {
                        return testFactory;
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return TestConfig.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("TestSupplierFactory");
                    }
                };
            }
        };

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(testPfr, definition)) {
            assertThat(testFactory.wasPluginInstanceCalled()).isTrue();
        }
    }

    @Test
    void testFilterDispatchExecutorNotAvailableAtInitTime() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        @Plugin(configType = TestConfig.class)
        class ExecutorTestFactory implements ServerTlsCredentialSupplierFactory<TestConfig, TestConfig> {
            @Override
            public TestConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config) {
                // This should throw IllegalStateException
                assertThatThrownBy(context::filterDispatchExecutor)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("not available at factory initialization time");
                return config;
            }

            @Override
            public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestConfig initializationData) {
                return mock(ServerTlsCredentialSupplier.class);
            }
        }

        ExecutorTestFactory testFactory = new ExecutorTestFactory();

        PluginFactoryRegistry testPfr = new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String instanceName) {
                        return testFactory;
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return TestConfig.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("TestSupplierFactory");
                    }
                };
            }
        };

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(testPfr, definition)) {
            // Constructor should succeed (the assertion is inside initialize)
            assertThat(manager.isConfigured()).isTrue();
        }
    }

    @Test
    void testCloseIsIdempotent() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition);
        manager.close();
        manager.close();
        manager.close();

        // Close should be called only once on the factory
        assertThat(TestSupplierFactory.closeCallCount).isEqualTo(1);
    }

    @Test
    void testCloseWithNullDefinition() {
        TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, null);
        // Should not throw
        manager.close();
    }

    @Test
    void testFactoryLifecycleOrderIsCorrect() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            // After construction, initialize should have been called
            assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.createCallCount).isEqualTo(0);
            assertThat(TestSupplierFactory.closeCallCount).isEqualTo(0);

            // After first create
            manager.createSupplier(creationContext);
            assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.createCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.closeCallCount).isEqualTo(0);

            // After second create
            manager.createSupplier(creationContext);
            assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.createCallCount).isEqualTo(2);
            assertThat(TestSupplierFactory.closeCallCount).isEqualTo(0);
        }

        // After close
        assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
        assertThat(TestSupplierFactory.createCallCount).isEqualTo(2);
        assertThat(TestSupplierFactory.closeCallCount).isEqualTo(1);
    }

    @Test
    void testConfigValidationInInitialize() {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        @Plugin(configType = TestConfig.class)
        class ValidatingFactory implements ServerTlsCredentialSupplierFactory<TestConfig, TestConfig> {
            @Override
            public TestConfig initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config)
                    throws PluginConfigurationException {
                TestConfig validated = Plugins.requireConfig(this, config);
                if (validated.value() == null || validated.value().isEmpty()) {
                    throw new PluginConfigurationException("value must not be empty");
                }
                return validated;
            }

            @Override
            public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, TestConfig initializationData) {
                return mock(ServerTlsCredentialSupplier.class);
            }
        }

        ValidatingFactory factory = new ValidatingFactory();
        PluginFactoryRegistry testPfr = new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String instanceName) {
                        return factory;
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return TestConfig.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("ValidatingFactory");
                    }
                };
            }
        };

        TlsCredentialSupplierDefinition validDefinition = new TlsCredentialSupplierDefinition("ValidatingFactory", config);

        // Valid config should succeed
        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(testPfr, validDefinition)) {
            assertThat(manager.isConfigured()).isTrue();
        }

        // Invalid config should fail during construction
        TestConfig invalidConfig = new TestConfig("");
        TlsCredentialSupplierDefinition invalidDefinition = new TlsCredentialSupplierDefinition("ValidatingFactory", invalidConfig);

        assertThatThrownBy(() -> new TlsCredentialSupplierManager(testPfr, invalidDefinition))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("value must not be empty");
    }

    @Test
    void testFactoryWithSharedResources() {
        record SharedContext(TestConfig config, String sharedResource) {}

        @Plugin(configType = TestConfig.class)
        class SharedResourceFactory implements ServerTlsCredentialSupplierFactory<TestConfig, SharedContext> {
            private boolean closeCalled = false;

            @Override
            public SharedContext initialize(ServerTlsCredentialSupplierFactoryContext context, TestConfig config) {
                // Create shared resource during initialization
                String sharedResource = "SharedResource-" + config.value();
                return new SharedContext(config, sharedResource);
            }

            @Override
            public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, SharedContext initializationData) {
                // Create supplier using shared resource
                ServerTlsCredentialSupplier supplier = mock(ServerTlsCredentialSupplier.class);
                // In real implementation, supplier would use initializationData.sharedResource()
                return supplier;
            }

            @Override
            public void close(SharedContext initializationData) {
                // Clean up shared resource
                closeCalled = true;
                assertThat(initializationData.sharedResource()).isEqualTo("SharedResource-test-value");
            }

            public boolean wasCloseCalled() {
                return closeCalled;
            }
        }

        TestConfig config = new TestConfig("test-value");
        SharedResourceFactory factory = new SharedResourceFactory();

        PluginFactoryRegistry testPfr = new PluginFactoryRegistry() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
                return new PluginFactory() {
                    @Override
                    public Object pluginInstance(String instanceName) {
                        return factory;
                    }

                    @Override
                    public Class<?> configType(String instanceName) {
                        return TestConfig.class;
                    }

                    @Override
                    public Set<String> registeredInstanceNames() {
                        return Set.of("SharedResourceFactory");
                    }
                };
            }
        };

        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("SharedResourceFactory", config);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(testPfr, definition)) {
            // Create multiple suppliers
            ServerTlsCredentialSupplier supplier1 = manager.createSupplier(creationContext);
            ServerTlsCredentialSupplier supplier2 = manager.createSupplier(creationContext);

            assertThat(supplier1).isNotNull();
            assertThat(supplier2).isNotNull();
        }

        // Verify close was called with the shared context
        assertThat(factory.wasCloseCalled()).isTrue();
    }

    @Test
    void testNullConfigForVoidConfigType() {
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("NoConfigSupplierFactory", null);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            assertThat(manager.isConfigured()).isTrue();

            // Should be able to create supplier without config
            ServerTlsCredentialSupplier supplier = manager.createSupplier(creationContext);
            assertThat(supplier).isNotNull();
        }
    }

    @Test
    void testConfigRequiredWhenFactoryExpectsIt() {
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", null);

        // Factory expects TestConfig but received null
        assertThatThrownBy(() -> new TlsCredentialSupplierManager(pfr, definition))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void testThreadSafetyOfCreateSupplier() throws Exception {
        TestConfig config = new TestConfig("test-value");
        TlsCredentialSupplierDefinition definition = new TlsCredentialSupplierDefinition("TestSupplierFactory", config);

        try (TlsCredentialSupplierManager manager = new TlsCredentialSupplierManager(pfr, definition)) {
            // Create suppliers from multiple threads concurrently
            int threadCount = 10;
            Thread[] threads = new Thread[threadCount];
            ServerTlsCredentialSupplier[] suppliers = new ServerTlsCredentialSupplier[threadCount];

            for (int i = 0; i < threadCount; i++) {
                int index = i;
                threads[i] = new Thread(() -> {
                    suppliers[index] = manager.createSupplier(creationContext);
                });
            }

            // Start all threads
            for (Thread thread : threads) {
                thread.start();
            }

            // Wait for all threads to complete
            for (Thread thread : threads) {
                thread.join();
            }

            // Verify all suppliers were created
            for (ServerTlsCredentialSupplier supplier : suppliers) {
                assertThat(supplier).isNotNull();
            }

            // Initialize called once, create called threadCount times
            assertThat(TestSupplierFactory.initializeCallCount).isEqualTo(1);
            assertThat(TestSupplierFactory.createCallCount).isEqualTo(threadCount);
        }
    }
}
