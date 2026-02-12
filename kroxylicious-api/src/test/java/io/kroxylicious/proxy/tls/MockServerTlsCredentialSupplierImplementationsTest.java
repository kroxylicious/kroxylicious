/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
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
 */
class MockServerTlsCredentialSupplierImplementationsTest {

    record SimpleConfig(String value) {}

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

    record SharedResourceConfig(String resourceName) {}

    record SharedContext(SharedResourceConfig config, String sharedResource) {}

    @Plugin(configType = SharedResourceConfig.class)
    public static class SharedResourceMockFactory implements ServerTlsCredentialSupplierFactory<SharedResourceConfig, SharedContext> {
        @Override
        public SharedContext initialize(ServerTlsCredentialSupplierFactoryContext context, SharedResourceConfig config)
                throws PluginConfigurationException {
            SharedResourceConfig validated = Plugins.requireConfig(this, config);
            return new SharedContext(validated, "SharedResource-" + validated.resourceName());
        }

        @Override
        public ServerTlsCredentialSupplier create(ServerTlsCredentialSupplierFactoryContext context, SharedContext initializationData) {
            return new MockSupplier(initializationData.sharedResource());
        }

        @Override
        public void close(SharedContext initializationData) {
        }
    }

    static class MockSupplier implements ServerTlsCredentialSupplier {
        private final String identifier;

        MockSupplier(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
            return CompletableFuture.completedFuture(mock(TlsCredentials.class));
        }

        String getIdentifier() {
            return identifier;
        }
    }

    public static class ClientContextAwareSupplier implements ServerTlsCredentialSupplier {
        private final TlsCredentials defaultCredentials;

        public ClientContextAwareSupplier(TlsCredentials defaultCredentials) {
            this.defaultCredentials = defaultCredentials;
        }

        @Override
        public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
            Optional<ClientTlsContext> clientContext = context.clientTlsContext();
            if (clientContext.isPresent() && clientContext.get().clientCertificate().isPresent()) {
                return CompletableFuture.completedFuture(mock(TlsCredentials.class));
            }
            return CompletableFuture.completedFuture(defaultCredentials);
        }
    }

    public static class FactoryBasedSupplier implements ServerTlsCredentialSupplier {
        private final PrivateKey key;
        private final Certificate[] chain;

        FactoryBasedSupplier(PrivateKey key, Certificate[] chain) {
            this.key = key;
            this.chain = chain;
        }

        @Override
        public CompletionStage<TlsCredentials> tlsCredentials(ServerTlsCredentialSupplierContext context) {
            TlsCredentials creds = context.tlsCredentials(key, chain);
            return CompletableFuture.completedFuture(creds);
        }
    }

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
        public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
            return mock(TlsCredentials.class);
        }
    }

    public static class MockSupplierContext implements ServerTlsCredentialSupplierContext {
        private final Optional<ClientTlsContext> clientContext;

        MockSupplierContext() {
            this(Optional.empty());
        }

        MockSupplierContext(Optional<ClientTlsContext> clientContext) {
            this.clientContext = clientContext;
        }

        @Override
        @NonNull
        public Optional<ClientTlsContext> clientTlsContext() {
            return clientContext;
        }

        @Override
        @NonNull
        public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
            return mock(TlsCredentials.class);
        }
    }

    // Tests

    @Test
    void testMockSupplierFactoryLifecycle() {
        MockSupplierFactory factory = new MockSupplierFactory();
        MockFactoryContext context = new MockFactoryContext();
        SimpleConfig config = new SimpleConfig("test-value");

        SimpleConfig initData = factory.initialize(context, config);
        assertThat(initData).isNotNull();
        assertThat(initData.value()).isEqualTo("test-value");

        ServerTlsCredentialSupplier supplier = factory.create(context, initData);
        assertThat(supplier).isNotNull().isInstanceOf(MockSupplier.class);
        assertThat(((MockSupplier) supplier).getIdentifier()).isEqualTo("test-value");
    }

    @Test
    void testNoConfigMockFactory() {
        NoConfigMockFactory factory = new NoConfigMockFactory();
        MockFactoryContext context = new MockFactoryContext();

        Void initData = factory.initialize(context, null);
        assertThat(initData).isNull();

        ServerTlsCredentialSupplier supplier = factory.create(context, initData);
        assertThat(supplier).isNotNull().isInstanceOf(MockSupplier.class);
    }

    @Test
    void testSharedResourceMockFactory() {
        SharedResourceMockFactory factory = new SharedResourceMockFactory();
        MockFactoryContext context = new MockFactoryContext();
        SharedResourceConfig config = new SharedResourceConfig("resource-1");

        SharedContext initData = factory.initialize(context, config);
        assertThat(initData).isNotNull();
        assertThat(initData.sharedResource()).isEqualTo("SharedResource-resource-1");

        ServerTlsCredentialSupplier supplier1 = factory.create(context, initData);
        ServerTlsCredentialSupplier supplier2 = factory.create(context, initData);
        assertThat(((MockSupplier) supplier1).getIdentifier()).isEqualTo("SharedResource-resource-1");
        assertThat(((MockSupplier) supplier2).getIdentifier()).isEqualTo("SharedResource-resource-1");

        factory.close(initData);
    }

    @Test
    void testMockSupplier() throws Exception {
        MockSupplier supplier = new MockSupplier("test-supplier");
        MockSupplierContext context = new MockSupplierContext();

        CompletionStage<TlsCredentials> result = supplier.tlsCredentials(context);
        assertThat(result.toCompletableFuture().get()).isNotNull();
    }

    @Test
    void testClientContextAwareSupplier() throws Exception {
        TlsCredentials mockDefaultCredentials = mock(TlsCredentials.class);
        ClientContextAwareSupplier supplier = new ClientContextAwareSupplier(mockDefaultCredentials);
        MockSupplierContext contextWithoutClient = new MockSupplierContext();

        CompletionStage<TlsCredentials> result1 = supplier.tlsCredentials(contextWithoutClient);
        assertThat(result1.toCompletableFuture().get()).isSameAs(mockDefaultCredentials);

        ClientTlsContext mockClientContext = mock(ClientTlsContext.class);
        MockSupplierContext contextWithClient = new MockSupplierContext(Optional.of(mockClientContext));

        CompletionStage<TlsCredentials> result2 = supplier.tlsCredentials(contextWithClient);
        assertThat(result2.toCompletableFuture().get()).isNotNull();
    }

    @Test
    void testFactoryBasedSupplier() throws Exception {
        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };
        MockSupplierContext context = new MockSupplierContext();
        FactoryBasedSupplier supplier = new FactoryBasedSupplier(mockKey, mockChain);

        CompletionStage<TlsCredentials> result = supplier.tlsCredentials(context);
        assertThat(result.toCompletableFuture().get()).isNotNull();
    }

    @Test
    void testMockFactoryContextPluginComposition() {
        MockFactoryContext context = new MockFactoryContext();
        assertThat(context.pluginInstance(Object.class, "SomePlugin")).isNull();
        assertThat(context.pluginImplementationNames(Object.class)).isEmpty();
    }

    @Test
    void testMockSupplierContextWithClientContext() {
        ClientTlsContext mockClientContext = mock(ClientTlsContext.class);
        MockSupplierContext context = new MockSupplierContext(Optional.of(mockClientContext));
        assertThat(context.clientTlsContext()).isPresent();
        assertThat(context.clientTlsContext().get()).isSameAs(mockClientContext);
    }

    @Test
    void testMockSupplierContextWithoutClientContext() {
        MockSupplierContext context = new MockSupplierContext();
        assertThat(context.clientTlsContext()).isEmpty();
    }

    @Test
    void testMockSupplierFactoryRequiresConfig() {
        MockSupplierFactory factory = new MockSupplierFactory();
        MockFactoryContext context = new MockFactoryContext();
        assertThatThrownBy(() -> factory.initialize(context, null))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void testMockFactoryWithDefaultCloseMethod() {
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
        }

        DefaultCloseFactory factory = new DefaultCloseFactory();
        SimpleConfig config = new SimpleConfig("test");
        factory.close(config);
    }
}
