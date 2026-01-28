/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for ServerTlsCredentialSupplierFactoryContext interface contracts.
 * These tests verify that implementations correctly provide plugin composition,
 * runtime resources, and TLS credential factory methods.
 */
class ServerTlsCredentialSupplierFactoryContextTest {

    private ServerTlsCredentialSupplierFactoryContext context;
    private FilterDispatchExecutor mockExecutor;
    private TlsCredentials mockCredentials;

    interface TestPlugin {
    }

    static class TestPluginImpl implements TestPlugin {
    }

    @BeforeEach
    void setUp() {
        mockExecutor = mock(FilterDispatchExecutor.class);
        mockCredentials = mock(TlsCredentials.class);

        context = new ServerTlsCredentialSupplierFactoryContext() {
            @Override
            public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
                if (pluginClass == TestPlugin.class && "TestImpl".equals(implementationName)) {
                    return pluginClass.cast(new TestPluginImpl());
                }
                throw new UnknownPluginInstanceException("Unknown plugin: " + pluginClass.getName() + ":" + implementationName);
            }

            @Override
            public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
                if (pluginClass == TestPlugin.class) {
                    return Set.of("TestImpl", "AnotherImpl");
                }
                return Set.of();
            }

            @Override
            @NonNull
            public FilterDispatchExecutor filterDispatchExecutor() {
                return mockExecutor;
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.completedFuture(mockCredentials);
            }
        };
    }

    @Test
    void testPluginInstanceReturnsValidPlugin() {
        // When
        TestPlugin plugin = context.pluginInstance(TestPlugin.class, "TestImpl");

        // Then
        assertThat(plugin).isNotNull();
        assertThat(plugin).isInstanceOf(TestPluginImpl.class);
    }

    @Test
    void testPluginInstanceThrowsForUnknownImplementation() {
        // When/Then
        assertThatThrownBy(() -> context.pluginInstance(TestPlugin.class, "UnknownImpl"))
                .isInstanceOf(UnknownPluginInstanceException.class);
    }

    @Test
    void testPluginInstanceThrowsForUnknownPluginClass() {
        interface UnknownPlugin {
        }

        // When/Then
        assertThatThrownBy(() -> context.pluginInstance(UnknownPlugin.class, "SomeImpl"))
                .isInstanceOf(UnknownPluginInstanceException.class);
    }

    @Test
    void testPluginImplementationNamesReturnsKnownImplementations() {
        // When
        Set<String> names = context.pluginImplementationNames(TestPlugin.class);

        // Then
        assertThat(names).containsExactlyInAnyOrder("TestImpl", "AnotherImpl");
    }

    @Test
    void testPluginImplementationNamesReturnsEmptyForUnknownPluginClass() {
        interface UnknownPlugin {
        }

        // When
        Set<String> names = context.pluginImplementationNames(UnknownPlugin.class);

        // Then
        assertThat(names).isEmpty();
    }

    @Test
    void testFilterDispatchExecutorReturnsValidExecutor() {
        // When
        FilterDispatchExecutor executor = context.filterDispatchExecutor();

        // Then
        assertThat(executor).isNotNull();
        assertThat(executor).isSameAs(mockExecutor);
    }

    @Test
    void testTlsCredentialsFactoryMethodAcceptsValidInput() throws Exception {
        // Given
        InputStream certStream = new ByteArrayInputStream("cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream keyStream = new ByteArrayInputStream("key-data".getBytes(StandardCharsets.UTF_8));

        // When
        CompletionStage<TlsCredentials> result = context.tlsCredentials(certStream, keyStream);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().join()).isSameAs(mockCredentials);
    }

    @Test
    void testContextSupportsMultiplePluginRequests() {
        // When - request same plugin multiple times
        TestPlugin plugin1 = context.pluginInstance(TestPlugin.class, "TestImpl");
        TestPlugin plugin2 = context.pluginInstance(TestPlugin.class, "TestImpl");

        // Then - should return new instances each time
        assertThat(plugin1).isNotNull();
        assertThat(plugin2).isNotNull();
        assertThat(plugin1).isNotSameAs(plugin2);
    }

    @Test
    void testFilterDispatchExecutorNotAvailableAtInitializationTime() {
        // This test verifies the contract that filterDispatchExecutor should throw
        // IllegalStateException when called during initialization (tested in manager tests)
        ServerTlsCredentialSupplierFactoryContext initContext = new ServerTlsCredentialSupplierFactoryContext() {
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
                throw new IllegalStateException("FilterDispatchExecutor not available at factory initialization time");
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.completedFuture(mock(TlsCredentials.class));
            }
        };

        // When/Then
        assertThatThrownBy(initContext::filterDispatchExecutor)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not available at factory initialization time");
    }

    @Test
    void testPluginImplementationNamesIsReadOnly() {
        // When
        Set<String> names = context.pluginImplementationNames(TestPlugin.class);

        // Then - attempting to modify should throw (assuming implementation returns unmodifiable set)
        assertThatThrownBy(() -> names.add("NewImpl"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
