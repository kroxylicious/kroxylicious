/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.assertj;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import io.kroxylicious.test.record.RecordTestUtils;

import static io.kroxylicious.test.assertj.Assertions.throwsAssertionErrorContaining;
import static io.kroxylicious.test.record.RecordTestUtils.record;

class RecordAssertTest {

    @Test
    void testRecordHasOffsetEqualTo() {
        Record record = record("KEY", "VALUE");
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        recordAssert.hasOffsetEqualTo(0);
        throwsAssertionErrorContaining(() -> recordAssert.hasOffsetEqualTo(1), "[record offset]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasOffsetEqualTo(1));
    }

    @Test
    void testRecordHasTimestampEqualTo() {
        Record record = record("KEY", "VALUE");
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        recordAssert.hasTimestampEqualTo(0);
        throwsAssertionErrorContaining(() -> recordAssert.hasTimestampEqualTo(1), "[record timestamp]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasTimestampEqualTo(1));
    }

    @Test
    void testRecordHasKeyEqualTo() {
        Record record = record("KEY", "VALUE");
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        recordAssert.hasKeyEqualTo("KEY");
        throwsAssertionErrorContaining(() -> recordAssert.hasKeyEqualTo("NOT_KEY"), "[record key]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasKeyEqualTo("NOT_KEY"));
    }

    @Test
    void testRecordHasNullKey() {
        Record record = record("KEY", "VALUE");
        Record nullKeyRecord = record(null, "VALUE");
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        KafkaAssertions.assertThat(nullKeyRecord).hasNullKey();
        throwsAssertionErrorContaining(recordAssert::hasNullKey, "[record key]");
        assertThrowsIfRecordNull(RecordAssert::hasNullValue);
    }

    @Test
    void testRecordHasValueEqualToString() {
        Record record = record("KEY", "VALUE");
        Record nullValue = record("KEY", (String) null);
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        RecordAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);
        recordAssert.hasValueEqualTo("VALUE");
        nullValueAssert.hasValueEqualTo((String) null);
        throwsAssertionErrorContaining(() -> recordAssert.hasValueEqualTo("NOT_VALUE"), "[record value]");
        throwsAssertionErrorContaining(() -> nullValueAssert.hasValueEqualTo("ANY"), "[record value]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasValueEqualTo("ANY"));
    }

    @Test
    void testRecordHasValueNotEqualToString() {
        Record record = record("KEY", "VALUE");
        Record nullValue = record("KEY", (String) null);
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        RecordAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);
        recordAssert.hasValueNotEqualTo("OTHER");
        nullValueAssert.hasValueNotEqualTo("OTHER");
        throwsAssertionErrorContaining(() -> recordAssert.hasValueNotEqualTo("VALUE"), "[record value]");
        throwsAssertionErrorContaining(() -> nullValueAssert.hasValueNotEqualTo(null), "[record value]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasValueNotEqualTo("ANY"));
    }

    @Test
    void testRecordHasNullValue() {
        Record record = record("KEY", "VALUE");
        Record nullValue = record("KEY", (String) null);
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        RecordAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);
        nullValueAssert.hasNullValue();
        throwsAssertionErrorContaining(recordAssert::hasNullValue, "[record value]");
        assertThrowsIfRecordNull(RecordAssert::hasNullValue);
    }

    @Test
    void testRecordHasValueEqualToByteArray() {
        Record record = record("KEY", "VALUE");
        Record nullValue = record("KEY", (String) null);
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        RecordAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);
        recordAssert.hasValueEqualTo("VALUE".getBytes(StandardCharsets.UTF_8));
        nullValueAssert.hasValueEqualTo((String) null);
        throwsAssertionErrorContaining(() -> recordAssert.hasValueEqualTo("NOT_VALUE".getBytes(StandardCharsets.UTF_8)), "[record value]");
        throwsAssertionErrorContaining(() -> nullValueAssert.hasValueEqualTo("ANY".getBytes(StandardCharsets.UTF_8)), "[record value]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasValueEqualTo("ANY".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testRecordHasValueEqualToRecord() {
        Record record = record("KEY", "VALUE");
        Record nullValue = record("KEY", (String) null);
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        RecordAssert nullValueAssert = KafkaAssertions.assertThat(nullValue);
        recordAssert.hasValueEqualTo(RecordTestUtils.record("KEY", "VALUE"));
        throwsAssertionErrorContaining(() -> recordAssert.hasValueEqualTo(RecordTestUtils.record("KEY", "NOT_VALUE")), "[record value]");
        throwsAssertionErrorContaining(() -> nullValueAssert.hasValueEqualTo(RecordTestUtils.record("KEY", "ANY")), "[record value]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasValueEqualTo(RecordTestUtils.record("KEY", "ANY")));
    }

    @Test
    void testRecordHasHeadersSize() {
        Record record = record("KEY", "VALUE", new RecordHeader("HEADER", "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        Record emptyHeaders = record("KEY", (String) null);
        RecordAssert recordAssert = KafkaAssertions.assertThat(record);
        RecordAssert emptyHeadersAssert = KafkaAssertions.assertThat(emptyHeaders);
        recordAssert.hasHeadersSize(1);
        emptyHeadersAssert.hasHeadersSize(0);
        throwsAssertionErrorContaining(() -> recordAssert.hasHeadersSize(2), "[record headers]");
        throwsAssertionErrorContaining(() -> emptyHeadersAssert.hasHeadersSize(1), "[record headers]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.hasHeadersSize(1));
    }

    @Test
    void testRecordContainsHeaderWithKey() {
        String headerKeyA = "HEADER_KEY_A";
        Record singleHeader = record("KEY", "VALUE", new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        String headerKeyB = "HEADER_KEY_B";
        Record multHeader = record(
                "KEY",
                "VALUE",
                new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(headerKeyB, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8))
        );
        Record emptyHeaders = record("KEY", (String) null);
        RecordAssert singleHeaderAssert = KafkaAssertions.assertThat(singleHeader);
        RecordAssert multiHeaderAssert = KafkaAssertions.assertThat(multHeader);
        RecordAssert emptyHeadersAssert = KafkaAssertions.assertThat(emptyHeaders);

        singleHeaderAssert.containsHeaderWithKey(headerKeyA);
        throwsAssertionErrorContaining(() -> singleHeaderAssert.containsHeaderWithKey("NOT_HEADER"), "[record headers]");

        multiHeaderAssert.containsHeaderWithKey(headerKeyA);
        multiHeaderAssert.containsHeaderWithKey(headerKeyB);
        throwsAssertionErrorContaining(() -> multiHeaderAssert.containsHeaderWithKey("NOT_HEADER"), "[record headers]");

        throwsAssertionErrorContaining(() -> emptyHeadersAssert.containsHeaderWithKey("ANY"), "[record headers]");
        assertThrowsIfRecordNull(nullAssert -> nullAssert.containsHeaderWithKey("ANY"));
    }

    @Test
    void testRecordFirstHeader() {
        String headerKeyA = "HEADER_KEY_A";
        Record singleHeader = record("KEY", "VALUE", new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        String headerKeyB = "HEADER_KEY_B";
        Record multiHeader = record(
                "KEY",
                "VALUE",
                new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(headerKeyB, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8))
        );
        Record emptyHeaders = record("KEY", (String) null);
        RecordAssert singleHeaderAssert = KafkaAssertions.assertThat(singleHeader);
        RecordAssert multiHeaderAssert = KafkaAssertions.assertThat(multiHeader);
        RecordAssert emptyHeadersAssert = KafkaAssertions.assertThat(emptyHeaders);

        singleHeaderAssert.firstHeader().hasKeyEqualTo(headerKeyA);
        multiHeaderAssert.firstHeader().hasKeyEqualTo(headerKeyA);
        throwsAssertionErrorContaining(emptyHeadersAssert::firstHeader, "[record headers]");
        assertThrowsIfRecordNull(RecordAssert::firstHeader);
    }

    @Test
    void testRecordLastHeader() {
        String headerKeyA = "HEADER_KEY_A";
        Record singleHeader = record("KEY", "VALUE", new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        String headerKeyB = "HEADER_KEY_B";
        Record multiHeader = record(
                "KEY",
                "VALUE",
                new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(headerKeyB, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8))
        );
        Record emptyHeaders = record("KEY", (String) null);
        RecordAssert singleHeaderAssert = KafkaAssertions.assertThat(singleHeader);
        RecordAssert multiHeaderAssert = KafkaAssertions.assertThat(multiHeader);
        RecordAssert emptyHeadersAssert = KafkaAssertions.assertThat(emptyHeaders);

        singleHeaderAssert.lastHeader().hasKeyEqualTo(headerKeyA);
        multiHeaderAssert.lastHeader().hasKeyEqualTo(headerKeyB);
        throwsAssertionErrorContaining(emptyHeadersAssert::lastHeader, "[record headers]");
        assertThrowsIfRecordNull(RecordAssert::lastHeader);
    }

    @Test
    void testRecordSingleHeader() {
        String headerKeyA = "HEADER_KEY_A";
        Record singleHeader = record("KEY", "VALUE", new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        String headerKeyB = "HEADER_KEY_B";
        Record multiHeader = record(
                "KEY",
                "VALUE",
                new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(headerKeyB, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8))
        );
        Record emptyHeaders = record("KEY", (String) null);
        RecordAssert singleHeaderAssert = KafkaAssertions.assertThat(singleHeader);
        RecordAssert multiHeaderAssert = KafkaAssertions.assertThat(multiHeader);
        RecordAssert emptyHeadersAssert = KafkaAssertions.assertThat(emptyHeaders);

        singleHeaderAssert.singleHeader().hasKeyEqualTo(headerKeyA);
        throwsAssertionErrorContaining(multiHeaderAssert::singleHeader, "[record headers]");
        throwsAssertionErrorContaining(emptyHeadersAssert::singleHeader, "[record headers]");
        assertThrowsIfRecordNull(RecordAssert::singleHeader);
    }

    @Test
    void testRecordHasEmptyHeaders() {
        String headerKeyA = "HEADER_KEY_A";
        Record singleHeader = record("KEY", "VALUE", new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)));
        String headerKeyB = "HEADER_KEY_B";
        Record multiHeader = record(
                "KEY",
                "VALUE",
                new RecordHeader(headerKeyA, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(headerKeyB, "HEADER_VALUE".getBytes(StandardCharsets.UTF_8))
        );
        Record emptyHeaders = record("KEY", (String) null);
        RecordAssert singleHeaderAssert = KafkaAssertions.assertThat(singleHeader);
        RecordAssert multiHeaderAssert = KafkaAssertions.assertThat(multiHeader);
        RecordAssert emptyHeadersAssert = KafkaAssertions.assertThat(emptyHeaders);

        emptyHeadersAssert.hasEmptyHeaders();
        throwsAssertionErrorContaining(multiHeaderAssert::hasEmptyHeaders, "[record headers]");
        throwsAssertionErrorContaining(singleHeaderAssert::hasEmptyHeaders, "[record headers]");
        assertThrowsIfRecordNull(RecordAssert::hasEmptyHeaders);
    }

    void assertThrowsIfRecordNull(ThrowingConsumer<RecordAssert> action) {
        RecordAssert recordAssert = KafkaAssertions.assertThat((Record) null);
        throwsAssertionErrorContaining(() -> action.accept(recordAssert), "[null record]");
    }
}
