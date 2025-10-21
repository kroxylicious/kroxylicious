/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.usernamespace;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class UserNamespaceTest {
    @Test
    @SuppressWarnings({ "unchecked", "resource" })
    void shouldInitAndCreateFilter() {
        var config = new UserNamespace.Config(Set.of(UserNamespace.ResourceType.GROUP_ID), "MAPPER", null);
        var userNamespace = new UserNamespace();
        var fc = mock(FilterFactoryContext.class);
        var mapperService = mock(ResourceNameMapperService.class);
        var mapper = mock(ResourceNameMapper.class);

        doReturn(mapperService).when(fc).pluginInstance(ResourceNameMapperService.class, "MAPPER");
        doReturn(mapper).when(mapperService).build();
        doReturn(mock(FilterDispatchExecutor.class)).when(fc).filterDispatchExecutor();

        var sec = userNamespace.initialize(fc, config);
        var filter = userNamespace.createFilter(fc, sec);
        assertThat(filter).isNotNull();
        verify(mapperService).initialize(null);
        userNamespace.close(sec);
    }

    @Test
    void topicMappingNotYetSupported() {
        var topicName = Set.of(UserNamespace.ResourceType.TOPIC_NAME);
        assertThatThrownBy(() -> new UserNamespace.Config(topicName, "MAPPER", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
