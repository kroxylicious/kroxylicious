/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DestroyableRawSecretKeyTest {

    @Test
    void destroy() {
        byte[] bytes = { 0, 1, 2 };
        var dk = DestroyableRawSecretKey.takeOwnershipOf("foo", bytes);
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
    void toDestroyableKey() {
        byte[] bytes1 = { 0, 1, 2 };
        var dk1 = DestroyableRawSecretKey.takeCopyOf("foo", bytes1);
        var dk2 = DestroyableRawSecretKey.toDestroyableKey(dk1);
        assertThat(dk1.isDestroyed()).isTrue();
        assertThat(dk2.isDestroyed()).isFalse();
        assertThat(dk2.getEncoded()).isEqualTo(bytes1);
        assertThat(dk2.getAlgorithm()).isEqualTo("foo");
    }

}
