/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MdsFailureTest {
    @Test
    void retainsFailureLocationWithoutExternalMessagesOrCauseChains() {
        // Given
        var original = new IOException("sensitive-response", new IllegalStateException("sensitive-cause"));
        original.addSuppressed(new IllegalArgumentException("sensitive-suppressed"));

        // When
        var safe = MdsFailure.safe(new CompletionException(original));
        var output = new StringWriter();
        safe.printStackTrace(new PrintWriter(output));

        // Then
        assertThat(safe.reason()).isEqualTo(MdsFailure.Reason.MDS_IO);
        assertThat(safe.errorType()).isEqualTo(IOException.class.getName());
        assertThat(safe.getStackTrace()).containsExactly(original.getStackTrace());
        assertThat(safe.getCause()).isNull();
        assertThat(safe.getSuppressed()).isEmpty();
        assertThat(output.toString()).contains("MDS_IO").doesNotContain("sensitive");
    }

    @Test
    void preservesTheUpstreamStageForTimeouts() {
        // Given
        var timeout = new CompletionException(new java.util.concurrent.TimeoutException("external-text"));

        // When
        var safe = MdsFailure.atStage(MdsFailure.Reason.UPSTREAM_AUTHENTICATE, timeout);

        // Then
        assertThat(safe.reason()).isEqualTo(MdsFailure.Reason.UPSTREAM_AUTHENTICATE);
        assertThat(safe).hasNoCause().hasMessageNotContaining("external-text");
    }
}
