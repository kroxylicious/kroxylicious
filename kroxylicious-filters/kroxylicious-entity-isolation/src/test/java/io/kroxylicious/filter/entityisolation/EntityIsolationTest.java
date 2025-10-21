/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class EntityIsolationTest {
    @Test
    @SuppressWarnings({ "unchecked", "resource" })
    void shouldInitAndCreateFilter() {
        var config = new EntityIsolation.Config(Set.of(EntityIsolation.ResourceType.GROUP_ID), "MAPPER", null);
        var resourceIsolation = new EntityIsolation();
        var fc = mock(FilterFactoryContext.class);
        var mapperService = mock(EntityNameMapperService.class);
        var mapper = mock(EntityNameMapper.class);

        doReturn(mapperService).when(fc).pluginInstance(EntityNameMapperService.class, "MAPPER");
        doReturn(mapper).when(mapperService).build();
        doReturn(mock(FilterDispatchExecutor.class)).when(fc).filterDispatchExecutor();

        var sec = resourceIsolation.initialize(fc, config);
        var filter = resourceIsolation.createFilter(fc, sec);
        assertThat(filter).isNotNull();
        verify(mapperService).initialize(null);
        resourceIsolation.close(sec);
    }

    @Test
    void topicMappingNotYetSupported() {
        var topicName = Set.of(EntityIsolation.ResourceType.TOPIC_NAME);
        assertThatThrownBy(() -> new EntityIsolation.Config(topicName, "MAPPER", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
