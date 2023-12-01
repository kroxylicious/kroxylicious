/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Describes how a record should be encrypted
 * @param kekId The KEK identifier to be used. Not null.
 * @param recordFields The fields of the record that should be encrypted with the given KEK. The empty set indicates that encryption should not be performed.
 * @param <K> The type of KEK identifier.
 */
public record EncryptionScheme<K>(
                                  @NonNull K kekId,
                                  @NonNull Set<RecordField> recordFields) {
    public EncryptionScheme {
        Objects.requireNonNull(kekId);
        Objects.requireNonNull(recordFields);
    }

    public static <K> EncryptionScheme<K> unencryptedScheme(K kekId) {
        return new EncryptionScheme<>(kekId, EnumSet.noneOf(RecordField.class));
    }

    public boolean requiresEncryption() {
        return !recordFields.isEmpty();
    }
}