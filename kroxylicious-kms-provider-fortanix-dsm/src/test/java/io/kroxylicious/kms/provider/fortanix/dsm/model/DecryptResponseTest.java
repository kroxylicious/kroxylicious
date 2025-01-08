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

class DecryptResponseTest {

    @Test
    void rejectsEmptyPlain() {
        assertThatThrownBy(() -> new DecryptResponse(null, new byte[]{}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringMasksPlaintext() {
        var keyMaterial = "mykey".getBytes(StandardCharsets.UTF_8);
        assertThat(new DecryptResponse("kid", keyMaterial))
                .hasToString("DecryptResponse{kid='kid', plain='*********'}");
    }

}
