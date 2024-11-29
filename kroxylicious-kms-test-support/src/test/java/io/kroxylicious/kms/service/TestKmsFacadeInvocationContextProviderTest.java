/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestKmsFacadeInvocationContextProviderTest {

    @Mock
    ExtensionContext extensionContext;

    @Mock
    ExtensionContext.Store store;

    @Mock
    TestKmsFacade<?, ?, ?> testKmsFacade;

    @SuppressWarnings("unchecked")
    @Test
    void shouldNotProvideRandomThings() {
        // Given
        when(extensionContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
        doReturn(Map.of()).when(store).getOrComputeIfAbsent(anyString(), any(), any(Class.class));

        final TestKmsFacadeInvocationContextProvider testKmsProvider = new TestKmsFacadeInvocationContextProvider();

        // When
        final boolean supportsParameter = testKmsProvider.supportsParameter(
                new MyParameterContext(String.class),
                extensionContext);

        // Then
        assertThat(supportsParameter).isFalse();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldProvideKmsThings() {
        // Given
        when(extensionContext.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
        doReturn(Map.of(TestKmsFacade.class, testKmsFacade)).when(store).getOrComputeIfAbsent(anyString(), any(), any(Class.class));

        final TestKmsFacadeInvocationContextProvider testKmsProvider = new TestKmsFacadeInvocationContextProvider();

        // When
        final boolean supportsParameter = testKmsProvider.supportsParameter(
                new MyParameterContext(TestKmsFacade.class),
                extensionContext);

        // Then
        assertThat(supportsParameter).isTrue();
    }

    private record MyParameterContext(Class<?> parameterType) implements ParameterContext {

        @Override
        public Parameter getParameter() {
            try {
                return Victim.class.getMethod("doStuffWith", parameterType).getParameters()[0];
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int getIndex() {
            return 0;
        }

        @Override
        public Optional<Object> getTarget() {
            return Optional.empty();
        }

        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        public static class Victim {

            private String stringField = "";
            private TestKmsFacade<?, ?, ?> testKmsFacade;

            public void doStuffWith(String arg) {
                stringField = arg;
            }

            public void doStuffWith(TestKmsFacade<?, ?, ?> arg) {
                this.testKmsFacade = arg;
            }
        }
    }
}