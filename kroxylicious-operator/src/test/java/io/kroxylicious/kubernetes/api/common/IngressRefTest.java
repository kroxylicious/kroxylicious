/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IngressRefTest {

    @Test
    void shouldEqualAKafkaKindedAnyRef() {
        var ingressRefFoo = new IngressRefBuilder().withName("foo").build();
        var anyFoo = new AnyLocalRefBuilder().withName("foo").withKind(ingressRefFoo.getKind()).withGroup(ingressRefFoo.getGroup()).build();
        assertThat(ingressRefFoo).isEqualTo(anyFoo);
        assertThat(anyFoo).isEqualTo(ingressRefFoo);
        assertThat(ingressRefFoo).hasSameHashCodeAs(anyFoo);
    }

}