/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.EnumSet;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.KekId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EncryptionSchemeTest {

    @Test
    void shouldRejectInvalidConstructorArgs() {
        EnumSet<RecordField> nonEmpty = EnumSet.of(RecordField.RECORD_VALUE);
        var empty = EnumSet.noneOf(RecordField.class);
        assertThrows(NullPointerException.class, () -> new EncryptionScheme<>(null, nonEmpty));
        Object kekId = new Object();
        assertThrows(NullPointerException.class, () -> new EncryptionScheme<>(new MyKekId(kekId), null));
        assertThrows(IllegalArgumentException.class, () -> new EncryptionScheme<>(new MyKekId(kekId), empty));
    }

    @Test
    void shouldAcceptValidConstructorArgs() {
        EnumSet<RecordField> nonEmpty = EnumSet.of(RecordField.RECORD_VALUE);
        Object kekId = new Object();
        var es = new EncryptionScheme<>(new MyKekId(kekId), nonEmpty);
        assertEquals(new MyKekId(kekId), es.kekId());
        assertEquals(nonEmpty, es.recordFields());
    }

    private static class MyKekId implements KekId<Object> {
        private final Object kekId;

        MyKekId(Object kekId) {
            this.kekId = kekId;
        }

        @Override
        public Object getId() {
            return kekId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyKekId myKekId = (MyKekId) o;

            return Objects.equals(kekId, myKekId.kekId);
        }

        @Override
        public int hashCode() {
            return kekId != null ? kekId.hashCode() : 0;
        }
    }
}