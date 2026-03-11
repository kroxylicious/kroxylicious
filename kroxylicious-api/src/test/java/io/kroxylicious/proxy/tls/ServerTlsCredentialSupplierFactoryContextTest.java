/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ServerTlsCredentialSupplierFactoryContextTest {

    private ServerTlsCredentialSupplierFactoryContext context;
    private FilterDispatchExecutor mockExecutor;

    interface TestPlugin {
    }

    static class TestPluginImpl implements TestPlugin {
    }

    @BeforeEach
    void setUp() {
        mockExecutor = mock(FilterDispatchExecutor.class);

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
        };
    }

    @Test
    void testPluginInstanceReturnsValidPlugin() {
        TestPlugin plugin = context.pluginInstance(TestPlugin.class, "TestImpl");
        assertThat(plugin).isNotNull().isInstanceOf(TestPluginImpl.class);
    }

    @Test
    void testPluginInstanceThrowsForUnknownImplementation() {
        assertThatThrownBy(() -> context.pluginInstance(TestPlugin.class, "UnknownImpl"))
                .isInstanceOf(UnknownPluginInstanceException.class);
    }

    @Test
    void testPluginInstanceThrowsForUnknownPluginClass() {
        interface UnknownPlugin {
        }
        assertThatThrownBy(() -> context.pluginInstance(UnknownPlugin.class, "SomeImpl"))
                .isInstanceOf(UnknownPluginInstanceException.class);
    }

    @Test
    void testPluginImplementationNamesReturnsKnownImplementations() {
        Set<String> names = context.pluginImplementationNames(TestPlugin.class);
        assertThat(names).containsExactlyInAnyOrder("TestImpl", "AnotherImpl");
    }

    @Test
    void testPluginImplementationNamesReturnsEmptyForUnknownPluginClass() {
        interface UnknownPlugin {
        }
        assertThat(context.pluginImplementationNames(UnknownPlugin.class)).isEmpty();
    }

    @Test
    void testFilterDispatchExecutorReturnsValidExecutor() {
        assertThat(context.filterDispatchExecutor()).isSameAs(mockExecutor);
    }

    @Test
    void testContextSupportsMultiplePluginRequests() {
        TestPlugin plugin1 = context.pluginInstance(TestPlugin.class, "TestImpl");
        TestPlugin plugin2 = context.pluginInstance(TestPlugin.class, "TestImpl");
        assertThat(plugin1).isNotNull();
        assertThat(plugin2).isNotNull();
        assertThat(plugin1).isNotSameAs(plugin2);
    }

    @Test
    void testFilterDispatchExecutorNotAvailableAtInitializationTime() {
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
        };

        assertThatThrownBy(initContext::filterDispatchExecutor)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not available at factory initialization time");
    }

    @Test
    void testPluginImplementationNamesIsReadOnly() {
        Set<String> names = context.pluginImplementationNames(TestPlugin.class);
        assertThatThrownBy(() -> names.add("NewImpl"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
