/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandlerFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link SaslTermination} filter factory.
 */
class SaslTerminationTest {

    @Test
    void shouldCloseHandlerFactoryOnFactoryClose() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), null, java.time.Clock.systemUTC());

        var factory = new SaslTermination();

        // When
        factory.close(context);

        // Then
        verify(handlerFactory).close();
    }

    @Test
    void shouldCloseAllHandlerFactoriesOnFactoryClose() {
        // Given
        var factory1 = mock(MechanismHandlerFactory.class);
        var factory2 = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", factory1, "SCRAM-SHA-512", factory2), null, java.time.Clock.systemUTC());

        var saslTermination = new SaslTermination();

        // When
        saslTermination.close(context);

        // Then
        verify(factory1).close();
        verify(factory2).close();
    }

    @Test
    void shouldSuppressExceptionsWhenClosingFactory() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        RuntimeException exception = new RuntimeException("Factory failed to close");
        doThrow(exception).when(handlerFactory).close();

        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), null, java.time.Clock.systemUTC());

        var factory = new SaslTermination();

        // When/Then
        assertThatThrownBy(() -> factory.close(context))
                .isSameAs(exception);
    }

    @Test
    void shouldCreateFilterFromContext() {
        // Given
        var handlerFactory = mock(MechanismHandlerFactory.class);
        var context = new SaslTermination.SaslTerminationContext(
                Map.of("SCRAM-SHA-256", handlerFactory), null, java.time.Clock.systemUTC());
        var filterFactoryContext = mock(FilterFactoryContext.class);

        var factory = new SaslTermination();

        // When
        var filter = factory.createFilter(filterFactoryContext, context);

        // Then
        assertThat(filter).isNotNull();
    }
}
