/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.usernamespace;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserPrincipalPrefixingResourceNameMapperTest {

    private static final String PRINCIPAL_NAME = "bob";
    private final UserPrincipalPrefixingResourceNameMapper mapper = new UserPrincipalPrefixingResourceNameMapper();
    @Mock
    Subject subject;

    @Mock
    User user;

    @BeforeEach
    void beforeEach() {
        when(user.name()).thenReturn(PRINCIPAL_NAME);
        when(subject.uniquePrincipalOfType(User.class)).thenReturn(Optional.of(user));
    }

    @Test
    void map() {
        var mapperContext = buildMapperContext(subject);
        assertThat(mapper.map(mapperContext, UserNamespace.ResourceType.TOPIC_NAME, "foo"))
                .isEqualTo("bob-foo");
    }

    @Test
    void unmap() {
        var mapperContext = buildMapperContext(subject);
        assertThat(mapper.unmap(mapperContext, UserNamespace.ResourceType.TOPIC_NAME, "bob-foo"))
                .isEqualTo("foo");
    }

    @Test
    void shouldRejectOutOfNamespaceResourceName() {
        var mapperContext = buildMapperContext(subject);
        assertThatThrownBy(() -> mapper.unmap(mapperContext, UserNamespace.ResourceType.TOPIC_NAME, "alice-foo"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isInNamespace() {
        var mapperContext = buildMapperContext(subject);
        assertThat(mapper.isInNamespace(mapperContext, UserNamespace.ResourceType.TOPIC_NAME, "bob-foo"))
                .isTrue();
    }

    @Test
    void notInNamespace() {
        assertThat(mapper.isInNamespace(buildMapperContext(subject), UserNamespace.ResourceType.TOPIC_NAME, "alice-foo"))
                .isFalse();
    }

    private MapperContext buildMapperContext(Subject s) {
        return new MapperContext(s, null, null);
    }

}
