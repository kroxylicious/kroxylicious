/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Objects;
import java.util.Set;

import io.kroxylicious.kms.service.KekId;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Describes how a record should be encrypted
 * @param kekId The KEK identifier to be used. Not null.
 * @param recordFields The fields of the record that should be encrypted with the given KEK. Neither null nor empty.
 */
public record EncryptionScheme(
                               @NonNull KekId kekId,
                               @NonNull Set<RecordField> recordFields) {
    public EncryptionScheme {
        Objects.requireNonNull(kekId);
        if (Objects.requireNonNull(recordFields).isEmpty()) {
            throw new IllegalArgumentException();
        }
    }

}
