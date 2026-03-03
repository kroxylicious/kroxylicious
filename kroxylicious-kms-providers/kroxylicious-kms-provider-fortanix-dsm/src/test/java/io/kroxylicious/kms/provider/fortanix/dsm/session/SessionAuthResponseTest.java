/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SessionAuthResponseTest {

    @Test
    void toStringMasksAccess() {
        assertThat(new SessionAuthResponse("type", 0, "accessToken", "entity", List.of()))
                .hasToString("SessionAuthResponse{tokenType='type', expiresIn=0, accessToken='*********', entityId='entity', allowedMfaMethods=[]}");
    }
}
