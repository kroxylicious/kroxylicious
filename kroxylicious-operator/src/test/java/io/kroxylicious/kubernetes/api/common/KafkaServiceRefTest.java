/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaServiceRefTest {

    // we knowingly use equals across types because we want the property that specific LocalRef types are equal to any other LocalRef
    // with the same group, kind and name.
    @SuppressWarnings("java:S5845")
    @Test
    void shouldEqualAnyRefWithSameCoordinates() {
        var serviceRefFoo = new KafkaServiceRefBuilder().withName("foo").build();
        var anyFoo = new AnyLocalRefBuilder().withName("foo").withKind(serviceRefFoo.getKind()).withGroup(serviceRefFoo.getGroup()).build();
        assertThat(serviceRefFoo).isEqualTo(anyFoo);
        assertThat(anyFoo).isEqualTo(serviceRefFoo);
        assertThat(serviceRefFoo).hasSameHashCodeAs(anyFoo);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldCompareEqualAnyRefWithSameCoordinates() {
        LocalRef refFoo = new KafkaServiceRefBuilder().withName("foo").build();
        LocalRef anyFoo = new AnyLocalRefBuilder().withName("foo").withKind(refFoo.getKind()).withGroup(refFoo.getGroup()).build();
        assertThat(refFoo).isEqualByComparingTo(anyFoo);
        assertThat(anyFoo).isEqualByComparingTo(refFoo);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldCompareLessThatAnyRefWithSameCoordinates() {
        LocalRef refFoo = new KafkaServiceRefBuilder().withName("foo").build();
        LocalRef anyFoo = new AnyLocalRefBuilder().withName("fooa").withKind(refFoo.getKind()).withGroup(refFoo.getGroup()).build();
        assertThat(refFoo).isLessThan(anyFoo);
        assertThat(anyFoo).isGreaterThan(refFoo);
    }

}