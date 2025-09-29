/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RefTest {

    @Test
    // we knowingly use equals across types because we want the property that specific LocalRef types are equal to any other LocalRef
    // with the same group, kind and name.
    @SuppressWarnings("java:S5845")
    void shouldRespectEqualsAndHashCode() {
        var secretRefFoo = new RefBuilder().withStrimziKafkaRef(new AnyLocalRefBuilder().withName("foo").withKind("Kafka").withGroup("").build())
                .withListenerName("listener").build();
        var secretRefFoo2 = new RefBuilder().withStrimziKafkaRef(new AnyLocalRefBuilder().withName("foo").withKind("Kafka").withGroup("").build())
                .withListenerName("listener").build();
        assertThat(secretRefFoo).isEqualTo(secretRefFoo);

        System.out.println(secretRefFoo.getStrimziKafkaRef().getName());
        assertThat(secretRefFoo).isEqualTo(secretRefFoo2);
        assertThat(secretRefFoo2).isEqualTo(secretRefFoo);
        assertThat(secretRefFoo).hasSameHashCodeAs(secretRefFoo2);

    }

    @Test
    void shouldReturnBuilder() {
        // Given
        RefBuilder originalBuilder = new RefBuilder();
        var fooRef = originalBuilder.withStrimziKafkaRef(new AnyLocalRefBuilder().withName("foo").build()).build();

        // When
        RefBuilder actualBuilder = fooRef.edit();

        // Then
        assertThat(actualBuilder)
                .isNotNull()
                .isInstanceOf(RefBuilder.class)
                .isNotSameAs(originalBuilder);
    }
}
