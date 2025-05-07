/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TrustAnchorRefTest {

    @Test
    // we knowingly use equals across types because we want the property that specific LocalRef types are equal to any other LocalRef
    // with the same group, kind and name.
    @SuppressWarnings("java:S5845")
    void shouldRespectEqualsAndHashCode() {
        var secretRefFoo = new TrustAnchorRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Secret").withGroup("").build()).withKey("key").build();
        var secretRefFoo2 = new TrustAnchorRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Secret").withGroup("").build()).withKey("key").build();
        var cmRefFoo = new TrustAnchorRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("ConfigMap").withGroup("").build()).withKey("key").build();
        var diffGroupSecretFoo = new TrustAnchorRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Secret").withGroup("not.the.usual.group").build())
                .withKey("key").build();
        var diffKeySecretFoo = new TrustAnchorRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Secret").withGroup("").build()).withKey("diff.key")
                .build();
        assertThat(secretRefFoo).isEqualTo(secretRefFoo);
        assertThat(secretRefFoo).isNotEqualTo("salami");
        assertThat(secretRefFoo).isNotEqualTo(diffGroupSecretFoo);
        assertThat(secretRefFoo).isNotEqualTo(diffKeySecretFoo);
        assertThat(secretRefFoo).isEqualTo(secretRefFoo2);
        assertThat(secretRefFoo2).isEqualTo(secretRefFoo);
        assertThat(secretRefFoo).hasSameHashCodeAs(secretRefFoo2);

        assertThat(secretRefFoo).isNotEqualTo(cmRefFoo);
    }

    @Test
    void shouldReturnBuilder() {
        // Given
        TrustAnchorRefBuilder originalBuilder = new TrustAnchorRefBuilder();
        var fooRef = originalBuilder.withRef(new AnyLocalRefBuilder().withName("foo").build()).build();

        // When
        TrustAnchorRefBuilder actualBuilder = fooRef.edit();

        // Then
        assertThat(actualBuilder)
                .isNotNull()
                .isInstanceOf(TrustAnchorRefBuilder.class)
                .isNotSameAs(originalBuilder);
    }
}
