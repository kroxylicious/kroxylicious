/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StrimziKafkaRefTest {

    @Test
    // we knowingly use equals across types because we want the property that specific LocalRef types are equal to any other LocalRef
    // with the same group, kind and name.
    @SuppressWarnings("java:S5845")
    void shouldRespectEqualsAndHashCode() {
        var secretRefFoo = new StrimziKafkaRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Kafka").build())
                .withListenerName("plain").build();
        var secretRefFoo2 = new StrimziKafkaRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Kafka").build())
                .withListenerName("plain").build();
        var diffRefKind = new StrimziKafkaRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("ConfigMap").build())
                .withListenerName("plain").build();
        var diffRefName = new StrimziKafkaRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Kafka").withGroup("not.the.usual.group").build())
                .withListenerName("plain").build();
        var diffRefListener = new StrimziKafkaRefBuilder().withRef(new AnyLocalRefBuilder().withName("foo").withKind("Kafka").withGroup("").build())
                .withListenerName("tls")
                .build();

        assertThat(secretRefFoo)
                .isNotEqualTo("salami")
                .isNotEqualTo(diffRefName)
                .isNotEqualTo(diffRefListener)
                .isEqualTo(secretRefFoo2)
                .isEqualTo(secretRefFoo)
                .isNotEqualTo(diffRefKind)
                .hasSameHashCodeAs(secretRefFoo2);
    }

    @Test
    void shouldReturnBuilder() {
        // Given
        StrimziKafkaRefBuilder originalBuilder = new StrimziKafkaRefBuilder();
        var fooRef = originalBuilder.withRef(new AnyLocalRefBuilder().withName("foo").build()).build();

        // When
        StrimziKafkaRefBuilder actualBuilder = fooRef.edit();

        // Then
        assertThat(actualBuilder)
                .isNotNull()
                .isInstanceOf(StrimziKafkaRefBuilder.class)
                .isNotSameAs(originalBuilder);
    }
}
