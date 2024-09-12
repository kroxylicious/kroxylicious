/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecretKeyUtilsTest {

    @Test
    void same() {
        byte[] bytes1 = { 0, 1, 2 };
        var dk1 = DestroyableRawSecretKey.takeOwnershipOf(bytes1, "foo");
        var dk2 = DestroyableRawSecretKey.takeCopyOf(bytes1, "foo");
        byte[] bytes3 = { 9, 8, 7 };
        var dk3 = DestroyableRawSecretKey.takeOwnershipOf(bytes3, "foo");

        assertThat(SecretKeyUtils.same(dk1, dk1)).isTrue();
        assertThat(SecretKeyUtils.same(dk1, dk2)).isTrue();
        assertThat(SecretKeyUtils.same(dk1, dk3)).isFalse();

        assertThat(SecretKeyUtils.same(dk2, dk2)).isTrue();
        assertThat(SecretKeyUtils.same(dk2, dk1)).isTrue();
        assertThat(SecretKeyUtils.same(dk2, dk3)).isFalse();

        assertThat(SecretKeyUtils.same(dk3, dk1)).isFalse();
        assertThat(SecretKeyUtils.same(dk3, dk2)).isFalse();
        assertThat(SecretKeyUtils.same(dk3, dk3)).isTrue();

        dk1.destroy();
        assertThatThrownBy(() -> SecretKeyUtils.same(dk1, dk3)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> SecretKeyUtils.same(dk3, dk1)).isInstanceOf(IllegalStateException.class);

    }

}
