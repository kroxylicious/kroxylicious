/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageEncryptionSchemeTest {

    @Test
    void shouldRejectInvalidConstructorArgs() {
        EnumSet<RecordField> nonEmpty = EnumSet.of(RecordField.RECORD_VALUE);
        var empty = EnumSet.noneOf(RecordField.class);
        assertThrows(NullPointerException.class, () -> new SingleKekMessageEncryptionScheme<>(null, nonEmpty));
        Object kekId = new Object();
        assertThrows(NullPointerException.class, () -> new SingleKekMessageEncryptionScheme<>(kekId, null));
        assertThrows(IllegalArgumentException.class, () -> new SingleKekMessageEncryptionScheme<>(kekId, empty));
    }

    @Test
    void shouldAcceptValidConstructorArgs() {
        EnumSet<RecordField> nonEmpty = EnumSet.of(RecordField.RECORD_VALUE);
        Object kekId = new Object();
        var es = new SingleKekMessageEncryptionScheme<>(kekId, nonEmpty);
        assertEquals(kekId, es.kekId());
        assertEquals(nonEmpty, es.recordFields());
    }

}
