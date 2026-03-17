/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.filter.entityisolation.EntityIsolation.EntityType;
import io.kroxylicious.filter.entityisolation.EntityNameMapper.UnacceptableEntityNameException;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrincipalEntityNameMapperTest {

    private static final String BOB_PRINCIPAL_NAME = "bob";
    private static final String ALICE_PRINCIPAL_NAME = "alice";

    @Mock(strictness = Mock.Strictness.LENIENT)
    Subject bobSubject;

    @Mock(strictness = Mock.Strictness.LENIENT)
    User bobUser;

    @Mock(strictness = Mock.Strictness.LENIENT)
    Subject aliceSubject;

    @Mock(strictness = Mock.Strictness.LENIENT)
    User aliceUser;

    private PrincipalEntityNameMapper mapper;

    @BeforeEach
    void beforeEach() {
        when(bobUser.name()).thenReturn(BOB_PRINCIPAL_NAME);
        when(bobSubject.uniquePrincipalOfType(User.class)).thenReturn(Optional.of(bobUser));
        when(aliceUser.name()).thenReturn(ALICE_PRINCIPAL_NAME);
        when(aliceSubject.uniquePrincipalOfType(User.class)).thenReturn(Optional.of(aliceUser));
        mapper = new PrincipalEntityNameMapper(User.class, "-");
    }

    @Test
    void map() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        // When
        var upstreamName = mapper.map(mapperContext, EntityType.TOPIC_NAME, "foo");
        // Then
        assertThat(upstreamName).isEqualTo("bob-foo");
    }

    @Test
    void unmap() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        // When
        String upstreamName = mapper.unmap(mapperContext, EntityType.TOPIC_NAME, "bob-foo");
        // Then
        assertThat(upstreamName).isEqualTo("foo");
    }

    @Test
    void shouldRejectPrincipalContainingSeparator() {
        // Given
        when(bobUser.name()).thenReturn("dash-boy");
        var mapperContext = buildMapperContext(bobSubject);
        // When/Then
        assertThatThrownBy(() -> mapper.map(mapperContext, EntityType.TOPIC_NAME, "foo"))
                .isInstanceOf(UnacceptableEntityNameException.class)
                .hasMessageContaining("Principal name 'dash-boy' is unaccepted as it contains the separator '-'");
    }

    @Test
    void shouldRejectUpstreamNameOwnedByAnotherContext() {
        // Given
        var aliceContext = buildMapperContext(aliceSubject);
        var bobContext = buildMapperContext(bobSubject);

        var aliceUpstreamResource = mapper.map(aliceContext, EntityType.TOPIC_NAME, "foo");

        // When / Then
        assertThatThrownBy(() -> mapper.unmap(bobContext, EntityType.TOPIC_NAME, aliceUpstreamResource))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isOwnedByContext() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        // When/Then
        assertThat(mapper.isOwnedByContext(mapperContext, EntityType.TOPIC_NAME, "bob-foo"))
                .isTrue();
    }

    @Test
    void isNotOwnedByContext() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        // When/Then
        assertThat(mapper.isOwnedByContext(mapperContext, EntityType.TOPIC_NAME, "alice-foo"))
                .isFalse();
    }

    @Test
    void shouldRejectNonUniquePrincipal() {
        // Given
        var notAUniquePrincipal = mock(Principal.class).getClass();

        // When/Then
        assertThatThrownBy(() -> new PrincipalEntityNameMapper(notAUniquePrincipal, "-"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptySeparator() {
        // Given/When/Then
        assertThatThrownBy(() -> new PrincipalEntityNameMapper(User.class, ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private MapperContext buildMapperContext(Subject s) {
        return new MapperContext(s, null, null);
    }
}
