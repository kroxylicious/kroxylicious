/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Tests for mock implementations of ServerTlsCredentialSupplier and related interfaces.
 * These mocks can be used by plugin authors for testing their implementations.
 */
class MockServerTlsCredentialSupplierImplementationsTest {

    // Simple mock configuration
    record SimpleConfig(String value) {}

    // Mock factory with configuration
    @Plugin(configType = SimpleConfig.class)
    public static class MockSupplierFactory implements ServerTlsCredentialSupplierFactory<SimpleConfig, SimpleConfig> {

        @Override
        public SimpleConfig initialize(ServerTlsCredentialSupplierFactoryContext context, SimpleConfig config)
                throws PluginConfigurationException {
            return Plugins.requireConfig(this, config);
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, SimpleConfig initializationData) {
            return new MockSupplier(initializationData.value());
        }
    }

    // Mock factory without configuration
    @Plugin(configType = Void.class)
    public static class NoConfigMockFactory implements ServerTlsCredentialSupplierFactory<Void, Void> {

        @Override
        public Void initialize(ServerTlsCredentialSupplierFactoryContext context, Void config) {
            return null;
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, Void initializationData) {
            return new MockSupplier("no-config");
        }
    }

    // Mock factory with shared resources
    record SharedResourceConfig(String resourceName) {}

    record SharedContext(SharedResourceConfig config, String sharedResource) {}

    @Plugin(configType = SharedResourceConfig.class)
    public static class SharedResourceMockFactory implements ServerTlsCredentialSupplierFactory<SharedResourceConfig, SharedContext> {

        @Override
        public SharedContext initialize(ServerTlsCredentialSupplierFactoryContext context, SharedResourceConfig config)
                throws PluginConfigurationException {
            SharedResourceConfig validated = Plugins.requireConfig(this, config);
            String resource = "SharedResource-" + validated.resourceName();
            return new SharedContext(validated, resource);
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, SharedContext initializationData) {
            return new MockSupplier(initializationData.sharedResource());
        }

        @Override
        public void close(SharedContext initializationData) {
            // Clean up shared resource
            // In real implementation, would close connections, etc.
        }
    }

    // Mock supplier implementation
    static class MockSupplier implements ServerTlsCredentialSupplier {
        private final String identifier;

        MockSupplier(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
            // Mock implementation returns mock credentials
            TlsCredentials mockCredentials = mock(TlsCredentials.class);
            return CompletableFuture.completedFuture(mockCredentials);
        }

        String getIdentifier() {
            return identifier;
        }
    }

    // Mock supplier that uses client context
    public static class ClientContextAwareSupplier implements ServerTlsCredentialSupplier {
        private final TlsCredentials defaultCredentials;

        public ClientContextAwareSupplier(TlsCredentials defaultCredentials) {
            this.defaultCredentials = defaultCredentials;
        }

        @Override
        public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
            Optional<ClientTlsContext> clientContext = context.clientTlsContext();
            if (clientContext.isPresent() && clientContext.get().clientCertificate().isPresent()) {
                // Use client certificate info to select credentials
                return CompletableFuture.completedFuture(mock(TlsCredentials.class));
            }
            // Fall back to default credentials
            return CompletableFuture.completedFuture(defaultCredentials);
        }
    }

    // Mock supplier that uses factory context to create credentials
    public static class FactoryBasedSupplier implements ServerTlsCredentialSupplier {
        private final ServerTlsCredentialSupplierContext factoryContext;

        FactoryBasedSupplier(ServerTlsCredentialSupplierContext factoryContext) {
            this.factoryContext = factoryContext;
        }

        @Override
        public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
            // Use factory context to create credentials from PEM data
            byte[] cert = "cert-data".getBytes(StandardCharsets.UTF_8);
            byte[] key = "key-data".getBytes(StandardCharsets.UTF_8);
            return context.tlsCredentials(cert, key);
        }
    }

    // Mock factory context for testing
    public static class MockFactoryContext implements ServerTlsCredentialSupplierFactoryContext {
        @Override
        public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
            return null;
        }

        @Override
        public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
            return Set.of();
        }

        @Override
        @NonNull
        public FilterDispatchExecutor filterDispatchExecutor() {
            return mock(FilterDispatchExecutor.class);
        }

        @Override
        @NonNull
        public CompletionStage<TlsCredentials> tlsCredentials(@NonNull byte[] certificateChainPem, @NonNull byte[] privateKeyPem, char[] password) {
            return CompletableFuture.completedFuture(mock(TlsCredentials.class));
        }
    }

    // Mock supplier context for testing
    public static class MockSupplierContext implements ServerTlsCredentialSupplierContext {
        private final Optional<ClientTlsContext> clientContext;
        private final TlsCredentials defaultCredentials;

        MockSupplierContext() {
            this(Optional.empty(), mock(TlsCredentials.class));
        }

        MockSupplierContext(Optional<ClientTlsContext> clientContext, TlsCredentials defaultCredentials) {
            this.clientContext = clientContext;
            this.defaultCredentials = defaultCredentials;
        }

        @Override
        @NonNull
        public Optional<ClientTlsContext> clientTlsContext() {
            return clientContext;
        }

        @Override
        @NonNull
        public CompletionStage<TlsCredentials> tlsCredentials(@NonNull byte[] certificateChainPem, @NonNull byte[] privateKeyPem, char[] password) {
            return CompletableFuture.completedFuture(mock(TlsCredentials.class));
        }
    }

    // Tests

    @Test
    void testMockSupplierFactoryLifecycle() throws Exception {
        // Given
        MockSupplierFactory factory = new MockSupplierFactory();
        MockFactoryContext context = new MockFactoryContext();
        SimpleConfig config = new SimpleConfig("test-value");

        // When - initialize
        SimpleConfig initData = factory.initialize(context, config);

        // Then
        assertThat(initData).isNotNull();
        assertThat(initData.value()).isEqualTo("test-value");

        // When - create
        ServerTlsCredentialSupplier supplier = factory.create(context, initData);

        // Then
        assertThat(supplier).isNotNull();
        assertThat(supplier).isInstanceOf(MockSupplier.class);
        assertThat(((MockSupplier) supplier).getIdentifier()).isEqualTo("test-value");
    }

    @Test
    void testNoConfigMockFactory() throws Exception {
        // Given
        NoConfigMockFactory factory = new NoConfigMockFactory();
        MockFactoryContext context = new MockFactoryContext();

        // When - initialize with null config
        Void initData = factory.initialize(context, null);

        // Then
        assertThat(initData).isNull();

        // When - create
        ServerTlsCredentialSupplier supplier = factory.create(context, initData);

        // Then
        assertThat(supplier).isNotNull();
        assertThat(supplier).isInstanceOf(MockSupplier.class);
    }

    @Test
    void testSharedResourceMockFactory() throws Exception {
        // Given
        SharedResourceMockFactory factory = new SharedResourceMockFactory();
        MockFactoryContext context = new MockFactoryContext();
        SharedResourceConfig config = new SharedResourceConfig("resource-1");

        // When - initialize
        SharedContext initData = factory.initialize(context, config);

        // Then - shared resource is created
        assertThat(initData).isNotNull();
        assertThat(initData.sharedResource()).isEqualTo("SharedResource-resource-1");

        // When - create multiple suppliers
        ServerTlsCredentialSupplier supplier1 = factory.create(context, initData);
        ServerTlsCredentialSupplier supplier2 = factory.create(context, initData);

        // Then - both use the same shared resource
        assertThat(supplier1).isInstanceOf(MockSupplier.class);
        assertThat(supplier2).isInstanceOf(MockSupplier.class);
        assertThat(((MockSupplier) supplier1).getIdentifier()).isEqualTo("SharedResource-resource-1");
        assertThat(((MockSupplier) supplier2).getIdentifier()).isEqualTo("SharedResource-resource-1");

        // When - close
        factory.close(initData);
        // Then - no exception (cleanup successful)
    }

    @Test
    void testMockSupplier() throws Exception {
        // Given
        MockSupplier supplier = new MockSupplier("test-supplier");
        MockSupplierContext context = new MockSupplierContext();

        // When
        CompletionStage<TlsCredentials> result = supplier.tlsCredentials(context);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().get()).isNotNull();
    }

    @Test
    void testClientContextAwareSupplier() throws Exception {
        // Given - supplier and context without client certificate
        TlsCredentials mockDefaultCredentials = mock(TlsCredentials.class);
        ClientContextAwareSupplier supplier = new ClientContextAwareSupplier(mockDefaultCredentials);
        MockSupplierContext contextWithoutClient = new MockSupplierContext();

        // When
        CompletionStage<TlsCredentials> result1 = supplier.tlsCredentials(contextWithoutClient);

        // Then - falls back to default credentials
        assertThat(result1).isNotNull();
        assertThat(result1.toCompletableFuture().get()).isSameAs(mockDefaultCredentials);

        // Given - context with client certificate
        ClientTlsContext mockClientContext = mock(ClientTlsContext.class);
        MockSupplierContext contextWithClient = new MockSupplierContext(
                Optional.of(mockClientContext),
                mock(TlsCredentials.class));

        // When
        CompletionStage<TlsCredentials> result2 = supplier.tlsCredentials(contextWithClient);

        // Then - uses client-specific credentials
        assertThat(result2).isNotNull();
        assertThat(result2.toCompletableFuture().get()).isNotNull();
    }

    @Test
    void testFactoryBasedSupplier() throws Exception {
        // Given
        MockSupplierContext context = new MockSupplierContext();
        FactoryBasedSupplier supplier = new FactoryBasedSupplier(context);

        // When
        CompletionStage<TlsCredentials> result = supplier.tlsCredentials(context);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().get()).isNotNull();
    }

    @Test
    void testMockFactoryContextPluginComposition() {
        // Given
        MockFactoryContext context = new MockFactoryContext();

        // When/Then - plugin instance returns null (not implemented)
        assertThat(context.pluginInstance(Object.class, "SomePlugin")).isNull();

        // When/Then - implementation names returns empty set
        assertThat(context.pluginImplementationNames(Object.class)).isEmpty();
    }

    @Test
    void testMockSupplierContextWithClientContext() {
        // Given
        ClientTlsContext mockClientContext = mock(ClientTlsContext.class);
        MockSupplierContext context = new MockSupplierContext(
                Optional.of(mockClientContext),
                mock(TlsCredentials.class));

        // When
        Optional<ClientTlsContext> result = context.clientTlsContext();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isSameAs(mockClientContext);
    }

    @Test
    void testMockSupplierContextWithoutClientContext() {
        // Given
        MockSupplierContext context = new MockSupplierContext();

        // When
        Optional<ClientTlsContext> result = context.clientTlsContext();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void testMockSupplierFactoryRequiresConfig() {
        // Given
        MockSupplierFactory factory = new MockSupplierFactory();
        MockFactoryContext context = new MockFactoryContext();

        // When/Then - null config should throw
        assertThatThrownBy(() -> factory.initialize(context, null))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void testMockFactoryWithDefaultCloseMethod() throws Exception {
        // Given - factory that doesn't override close()
        @Plugin(configType = SimpleConfig.class)
        class DefaultCloseFactory implements ServerTlsCredentialSupplierFactory<SimpleConfig, SimpleConfig> {
            @Override
            public SimpleConfig initialize(ServerTlsCredentialSupplierFactoryContext context, SimpleConfig config) {
                return config;
            }

            @Override
            public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, SimpleConfig initializationData) {
                return new MockSupplier(initializationData.value());
            }
            // close() not overridden - uses default no-op implementation
        }

        DefaultCloseFactory factory = new DefaultCloseFactory();
        SimpleConfig config = new SimpleConfig("test");

        // When/Then - close should not throw
        factory.close(config);
    }
}
