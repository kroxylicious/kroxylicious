/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IngressRefTest {

    // we knowingly use equals across types because we want the property that specific LocalRef types are equal to any other LocalRef
    // with the same group, kind and name.
    @SuppressWarnings("java:S5845")
    @Test
    void shouldEqualAnyRefWithSameCoordinates() {
        var ingressRefFoo = new IngressRefBuilder().withName("foo").build();
        var anyFoo = new AnyLocalRefBuilder().withName("foo").withKind(ingressRefFoo.getKind()).withGroup(ingressRefFoo.getGroup()).build();
        assertThat(ingressRefFoo).isEqualTo(anyFoo);
        assertThat(anyFoo).isEqualTo(ingressRefFoo);
        assertThat(ingressRefFoo).hasSameHashCodeAs(anyFoo);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldCompareEqualAnyRefWithSameCoordinates() {
        LocalRef refFoo = new IngressRefBuilder().withName("foo").build();
        LocalRef anyFoo = new AnyLocalRefBuilder().withName("foo").withKind(refFoo.getKind()).withGroup(refFoo.getGroup()).build();
        assertThat(refFoo).isEqualByComparingTo(anyFoo);
        assertThat(anyFoo).isEqualByComparingTo(refFoo);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldCompareLessThatAnyRefWithSameCoordinates() {
        LocalRef refFoo = new IngressRefBuilder().withName("foo").build();
        LocalRef anyFoo = new AnyLocalRefBuilder().withName("fooa").withKind(refFoo.getKind()).withGroup(refFoo.getGroup()).build();
        assertThat(refFoo).isLessThan(anyFoo);
        assertThat(anyFoo).isGreaterThan(refFoo);
    }

    @Test
    void shouldReturnBuilder() {
        // Given
        IngressRefBuilder originalBuilder = new IngressRefBuilder();
        var fooRef = originalBuilder.withName("foo").build();

        // When
        IngressRefBuilder actualBuilder = fooRef.edit();

        // Then
        assertThat(actualBuilder).isNotNull().isNotSameAs(originalBuilder);
    }

}