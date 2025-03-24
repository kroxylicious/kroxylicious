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

}