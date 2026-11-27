/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import java.util.Objects;

import io.kroxylicious.systemtests.api.FilterSpec;
import io.kroxylicious.testing.kms.TestKmsFacade;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * {@link FilterSpec} for the record-encryption filter.
 * Captures the KMS facade (which provides the KMS service class and config at deployment time)
 * and optional experimental caching configuration.
 *
 * @param testKmsFacade       the KMS test facade providing service class and configuration
 * @param experimentalKmsConfig optional experimental KMS caching config, or {@code null} for defaults
 */
public record RecordEncryptionFilterSpec(@NonNull TestKmsFacade<?, ?, ?> testKmsFacade,
                                         @Nullable ExperimentalKmsConfig experimentalKmsConfig)
        implements FilterSpec {

    public RecordEncryptionFilterSpec {
        Objects.requireNonNull(testKmsFacade, "testKmsFacade must not be null");
    }

    /**
     * Creates a spec with default experimental config.
     *
     * @param testKmsFacade the KMS test facade
     */
    public RecordEncryptionFilterSpec(@NonNull TestKmsFacade<?, ?, ?> testKmsFacade) {
        this(testKmsFacade, null);
    }
}
