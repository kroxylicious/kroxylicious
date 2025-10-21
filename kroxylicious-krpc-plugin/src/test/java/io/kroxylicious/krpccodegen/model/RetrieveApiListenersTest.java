/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.MessageSpecType;
import io.kroxylicious.krpccodegen.schema.RequestListenerType;

import freemarker.template.TemplateModelException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetrieveApiListenersTest {

    @Mock
    MessageSpec messageSpec;

    @Mock
    private MessageSpecModel messageSpecModel;

    @Test
    void shouldRetrieveApiListenerForKnownSpec() throws Exception {
        // Given
        when(messageSpec.apiKey()).thenReturn(Optional.of(ApiKeys.PRODUCE.id));
        when(messageSpec.type()).thenReturn(MessageSpecType.REQUEST);
        when(messageSpec.listeners()).thenReturn(List.of(RequestListenerType.BROKER));
        when(messageSpecModel.getAdaptedObject(MessageSpec.class)).thenReturn(messageSpec);

        var model = new RetrieveApiListeners(Set.of(messageSpec));
        var methodArgs = List.of(messageSpecModel);

        // When
        var result = model.exec(methodArgs);

        // Then
        assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.set(RequestListenerType.class))
                .containsExactly(RequestListenerType.BROKER);
    }

    @Test
    void shouldRejectRequestForUnknownSpec() {
        // Given
        when(messageSpec.apiKey()).thenReturn(Optional.of(ApiKeys.PRODUCE.id));
        when(messageSpec.type()).thenReturn(MessageSpecType.REQUEST);
        when(messageSpec.listeners()).thenReturn(List.of(RequestListenerType.BROKER));

        MessageSpec fetchSpec = mock(MessageSpec.class);
        when(fetchSpec.apiKey()).thenReturn(Optional.of(ApiKeys.FETCH.id));

        when(messageSpecModel.getAdaptedObject(MessageSpec.class)).thenReturn(fetchSpec);

        var model = new RetrieveApiListeners(Set.of(messageSpec));
        var methodArgs = List.of(messageSpecModel);

        // When/Then
        assertThatThrownBy(() -> model.exec(methodArgs))
                .isInstanceOf(TemplateModelException.class);

    }

}
