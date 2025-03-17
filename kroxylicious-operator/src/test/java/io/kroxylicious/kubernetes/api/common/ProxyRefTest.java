/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyRefTest {

    @Test
    void shouldEqualAKafkaKindedAnyRef() {
        var proxyRef = new ProxyRefBuilder().withName("foo").build();
        var anyFoo = new AnyLocalRefBuilder().withName("foo").withKind(proxyRef.getKind()).withGroup(proxyRef.getGroup()).build();
        assertThat(proxyRef).isEqualTo(anyFoo);
        assertThat(anyFoo).isEqualTo(proxyRef);
        assertThat(proxyRef).hasSameHashCodeAs(anyFoo);
    }

}