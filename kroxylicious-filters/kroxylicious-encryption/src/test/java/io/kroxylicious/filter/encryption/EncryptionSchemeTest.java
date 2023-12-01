/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EncryptionSchemeTest {

    @Test
    void shouldRejectInvalidConstructorArgs() {
        EnumSet<RecordField> nonEmpty = EnumSet.of(RecordField.RECORD_VALUE);
        assertThrows(NullPointerException.class, () -> new EncryptionScheme<>(null, nonEmpty));
        Object kekId = new Object();
        assertThrows(NullPointerException.class, () -> new EncryptionScheme<>(kekId, null));
    }

    @Test
    void shouldAcceptValidConstructorArgs() {
        EnumSet<RecordField> nonEmpty = EnumSet.of(RecordField.RECORD_VALUE);
        Object kekId = new Object();
        var es = new EncryptionScheme<>(kekId, nonEmpty);
        assertEquals(kekId, es.kekId());
        assertEquals(nonEmpty, es.recordFields());
    }

    @Test
    void shouldRequireEncryption() {
        // Given
        final EncryptionScheme<String> encryptionScheme = new EncryptionScheme<>("EncryptMe", EnumSet.of(RecordField.RECORD_VALUE));

        // When
        assertThat(encryptionScheme.requiresEncryption()).isTrue();

        // Then
    }

    @Test
    void shouldNotRequireEncryption() {
        // Given
        final EncryptionScheme<String> encryptionScheme = new EncryptionScheme<>("EncryptMe", EnumSet.noneOf(RecordField.class));

        // When
        assertThat(encryptionScheme.requiresEncryption()).isFalse();

        // Then
    }
}