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
import io.kroxylicious.filter.entityisolation.EntityNameMapper.EntityMapperException;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
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

    @Test
    void shouldValidateSuccessfully() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);

        // When / Then
        assertThatNoException().isThrownBy(() -> mapper.validateContext(mapperContext));
    }

    @Test
    void validateShouldRejectAnonymousSubject() {
        // Given
        var mapperContext = buildMapperContext(Subject.anonymous());

        // When/Then
        assertThatThrownBy(() -> mapper.validateContext(mapperContext))
                .isInstanceOf(EntityMapperException.class)
                .hasMessageContaining(
                        "The PrincipalEntityNameMapper requires an authenticated subject with a unique principal of type User with a non-empty name, got subject Subject[principals=[]]");
    }

    @Test
    void validateShouldRejectPrincipalContainingSeparator() {
        // Given
        when(bobSubject.uniquePrincipalOfType(User.class)).thenReturn(Optional.of(new User("dash-boy")));
        var mapperContext = buildMapperContext(bobSubject);

        // When/Then
        assertThatThrownBy(() -> mapper.validateContext(mapperContext))
                .isInstanceOf(EntityMapperException.class)
                .hasMessageContaining("Principal 'User[name=dash-boy]' is unacceptable as it contains the mapping separator '-'");
    }

    @Test
    void map() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        mapper.validateContext(mapperContext);

        // When
        var upstreamName = mapper.map(mapperContext, EntityType.TOPIC_NAME, "foo");
        // Then
        assertThat(upstreamName).isEqualTo("bob-foo");
    }

    @Test
    void unmap() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        mapper.validateContext(mapperContext);

        // When
        String upstreamName = mapper.unmap(mapperContext, EntityType.TOPIC_NAME, "bob-foo");
        // Then
        assertThat(upstreamName).isEqualTo("foo");
    }

    @Test
    void unmapShouldRejectUpstreamNameOwnedByAnotherContext() {
        // Given
        var aliceContext = buildMapperContext(aliceSubject);
        mapper.validateContext(aliceContext);

        var bobContext = buildMapperContext(bobSubject);
        mapper.validateContext(bobContext);

        var aliceUpstreamResource = mapper.map(aliceContext, EntityType.TOPIC_NAME, "foo");

        // When / Then
        assertThatThrownBy(() -> mapper.unmap(bobContext, EntityType.TOPIC_NAME, aliceUpstreamResource))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void isOwnedByContext() {
        // Given
        var mapperContext = buildMapperContext(bobSubject);
        mapper.validateContext(mapperContext);
        // When/Then
        assertThat(mapper.isOwnedByContext(mapperContext, EntityType.TOPIC_NAME, "bob-foo"))
                .isTrue();
    }

    @Test
    void isNotOwnedByContext() {
        // Given
        var aliceContext = buildMapperContext(aliceSubject);
        mapper.validateContext(aliceContext);

        var bobContext = buildMapperContext(bobSubject);
        mapper.validateContext(bobContext);

        var aliceUpstreamResource = mapper.map(aliceContext, EntityType.TOPIC_NAME, "foo");

        // When/Then
        assertThat(mapper.isOwnedByContext(bobContext, EntityType.TOPIC_NAME, aliceUpstreamResource)).isFalse();
    }

    private MapperContext buildMapperContext(Subject s) {
        return new MapperContext(s, null, null);
    }
}
