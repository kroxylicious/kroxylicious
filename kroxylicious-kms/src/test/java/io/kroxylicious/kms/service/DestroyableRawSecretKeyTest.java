/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DestroyableRawSecretKeyTest {

    @Test
    void testDestroy() {
        byte[] bytes = { 0, 1, 2 };
        var dk = DestroyableRawSecretKey.byOwnershipTransfer("foo", bytes);
        assertThat(dk.getFormat()).isEqualTo("RAW");
        assertThat(dk.getAlgorithm()).isEqualTo("foo");
        var encoded = dk.getEncoded();
        assertThat(dk.isDestroyed()).isFalse();
        dk.destroy();
        assertThat(dk.isDestroyed()).isTrue();
        assertThat(bytes).isEqualTo(new byte[]{ 0, 0, 0 });
        assertThatThrownBy(dk::getEncoded).isExactlyInstanceOf(IllegalStateException.class);
        dk.destroy(); // should be idempotent
        assertThat(encoded).isEqualTo(new byte[]{ 0, 1, 2 });
    }

    @Test
    void testHashcodeAndEquals() {
        byte[] bytes1 = { 0, 1, 2 };
        var dk1 = DestroyableRawSecretKey.byOwnershipTransfer("foo", bytes1);
        var dk2 = DestroyableRawSecretKey.byClone("foo", bytes1);
        byte[] bytes3 = { 9, 8, 7 };
        var dk3 = DestroyableRawSecretKey.byOwnershipTransfer("foo", bytes3);

        assertThat(dk1).isEqualTo(dk1)
                .isEqualTo(dk2)
                .isNotEqualTo(dk3);
        assertThat(dk2)
                .isEqualTo(dk2)
                .isEqualTo(dk1)
                .isNotEqualTo(dk3);
        assertThat(dk3)
                .isNotEqualTo(dk1)
                .isNotEqualTo(dk2);

        assertThat(dk1).hasSameHashCodeAs(dk2)
                .doesNotHaveSameHashCodeAs(dk3);
        assertThat(dk2).doesNotHaveSameHashCodeAs(dk3);
    }

    @Test
    void testToDestroyableKey() {
        byte[] bytes1 = { 0, 1, 2 };
        var dk1 = DestroyableRawSecretKey.byClone("foo", bytes1);
        var dk2 = DestroyableRawSecretKey.toDestroyableKey(dk1);
        assertThat(dk1.isDestroyed()).isTrue();
        assertThat(dk2.isDestroyed()).isFalse();
        assertThat(dk2.getEncoded()).isEqualTo(bytes1);
        assertThat(dk2.getAlgorithm()).isEqualTo("foo");
    }

}