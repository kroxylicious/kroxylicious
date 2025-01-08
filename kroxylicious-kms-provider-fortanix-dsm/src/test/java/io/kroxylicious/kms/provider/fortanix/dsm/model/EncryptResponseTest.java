/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.model;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptResponseTest {

    @Test
    void rejectsEmptyCiphertext() {
        assertThatThrownBy(() -> new EncryptResponse(null, new byte[]{}, "iv".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEmptyIv() {
        assertThatThrownBy(() -> new EncryptResponse(null, "cipher".getBytes(StandardCharsets.UTF_8), new byte[]{}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringMasksCipherAndIv() {
        assertThat(new EncryptResponse("kid", "cipher".getBytes(StandardCharsets.UTF_8), "iv".getBytes(StandardCharsets.UTF_8)))
                .hasToString("EncryptResponse{kid='kid', cipher='*********', iv='*********'}");
    }

}
