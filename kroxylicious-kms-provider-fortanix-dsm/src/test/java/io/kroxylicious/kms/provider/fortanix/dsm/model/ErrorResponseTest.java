/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 */
class ErrorResponseTest {

    @Test
    void notFound() {
        var er = new ErrorResponse("NotFoundException", null);
        assertThat(er.type()).isEqualTo("NotFoundException");
        assertThat(er.message()).isNull();
        assertThat(er.isNotFound()).isTrue();
    }

    @Test
    void pendingDeleteIsTreatedAsNotFound() {
        var er = new ErrorResponse("KMSInvalidStateException", "arn:aws:kms:us-east-1:000000000000:key/b3c06d76-25b1-41ad-b3d7-580ad1e58eeb is pending deletion.");
        assertThat(er.isNotFound()).isTrue();
    }

    @Test
    void accessDenied() {
        var er = new ErrorResponse("AccessDeniedException", null);
        assertThat(er.type()).isEqualTo("AccessDeniedException");
        assertThat(er.message()).isNull();
        assertThat(er.isNotFound()).isFalse();
    }
}