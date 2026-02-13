/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.resourceisolation;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ResourceIsolationTest {
    @Test
    @SuppressWarnings({ "unchecked", "resource" })
    void shouldInitAndCreateFilter() {
        var config = new ResourceIsolation.Config(Set.of(ResourceIsolation.ResourceType.GROUP_ID), "MAPPER", null);
        var resourceIsolation = new ResourceIsolation();
        var fc = mock(FilterFactoryContext.class);
        var mapperService = mock(ResourceNameMapperService.class);
        var mapper = mock(ResourceNameMapper.class);

        doReturn(mapperService).when(fc).pluginInstance(ResourceNameMapperService.class, "MAPPER");
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
        var topicName = Set.of(ResourceIsolation.ResourceType.TOPIC_NAME);
        assertThatThrownBy(() -> new ResourceIsolation.Config(topicName, "MAPPER", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
