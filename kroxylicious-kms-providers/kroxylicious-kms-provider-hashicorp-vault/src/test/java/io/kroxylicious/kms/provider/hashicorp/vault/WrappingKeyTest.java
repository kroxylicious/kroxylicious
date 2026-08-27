/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class WrappingKeyTest {

    @Test
    void equalWhenNameAndVersionMatch() {
        assertThat(new WrappingKey("key", 1)).isEqualTo(new WrappingKey("key", 1));
    }

    @Test
    void hashCodeConsistentWithEquals() {
        var a = new WrappingKey("key", 1);
        var b = new WrappingKey("key", 1);
        assertThat(a).hasSameHashCodeAs(b);
    }

    @Test
    void differentVersionsMeansNotEqual() {
        // EncryptionDekCache.CacheKey wraps WrappingKey; two keys with the same name but
        // different versions must be unequal so a rotation causes a cache miss.
        var beforeRotation = new WrappingKey("mykey", 1);
        var afterRotation = new WrappingKey("mykey", 2);
        assertThat(beforeRotation).isNotEqualTo(afterRotation);
    }

    @Test
    void differentNamesMeansNotEqual() {
        assertThat(new WrappingKey("key-a", 1)).isNotEqualTo(new WrappingKey("key-b", 1));
    }

    @Test
    void nullNameIsRejected() {
        assertThatNullPointerException()
                .isThrownBy(() -> new WrappingKey(null, 1))
                .withMessageContaining("name must not be null");
    }

    @Test
    void nameAndVersionAccessors() {
        var key = new WrappingKey("mykey", 3);
        assertThat(key.name()).isEqualTo("mykey");
        assertThat(key.version()).isEqualTo(3);
    }
}
