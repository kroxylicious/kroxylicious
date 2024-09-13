/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.reflect.ClassPath;

import io.kroxylicious.filter.encryption.config.ParcelVersion;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.kafka.transform.BatchAwareMemoryRecordsBuilder;
import io.kroxylicious.test.record.RecordTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParcelTest {

    static Stream<Arguments> shouldRoundTrip() {
        return Stream.of(
                Arguments.of(EnumSet.of(RecordField.RECORD_VALUE), RecordTestUtils.record((ByteBuffer) null)),
                Arguments.of(EnumSet.of(RecordField.RECORD_VALUE), RecordTestUtils.record(ByteBuffer.wrap(new byte[]{ 1, 2, 3 }))), // no headers
                Arguments.of(
                        EnumSet.of(RecordField.RECORD_VALUE),
                        RecordTestUtils.record(
                                ByteBuffer.wrap(new byte[]{ 1, 2, 3 }),
                                new RecordHeader("foo", null)
                        )
                ), // header with null value
                Arguments.of(
                        EnumSet.of(RecordField.RECORD_VALUE),
                        RecordTestUtils.record(
                                ByteBuffer.wrap(new byte[]{ 1, 2, 3 }),
                                new RecordHeader("foo", new byte[]{ 4, 5, 6 })
                        )
                ), // header with non-null value

                Arguments.of(EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES), RecordTestUtils.record((ByteBuffer) null)),
                Arguments.of(EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES), RecordTestUtils.record(ByteBuffer.wrap(new byte[]{ 1, 2, 3 }))),
                // no headers
                Arguments.of(
                        EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES),
                        RecordTestUtils.record(
                                ByteBuffer.wrap(new byte[]{ 1, 2, 3 }),
                                new RecordHeader("foo", null)
                        )
                ), // header with null value
                Arguments.of(
                        EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES),
                        RecordTestUtils.record(
                                ByteBuffer.wrap(new byte[]{ 1, 2, 3 }),
                                new RecordHeader("foo", new byte[]{ 4, 5, 6 })
                        )
                ) // header with non-null value
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldRoundTrip(Set<RecordField> fields, Record record) {
        var expectedValue = record.hasValue() ? record.value().duplicate() : null;
        Parcel parcel = ParcelVersionResolver.ALL.fromName(ParcelVersion.V1);
        int size = parcel.sizeOfParcel(fields, record);
        var buffer = ByteBuffer.allocate(size);
        parcel.writeParcel(fields, record, buffer);
        assertThat(buffer.remaining()).isZero();

        buffer.flip();

        BatchAwareMemoryRecordsBuilder mockBuilder = Mockito.mock(BatchAwareMemoryRecordsBuilder.class);
        parcel.readParcel(buffer, record, (v, h) -> mockBuilder.appendWithOffset(record.offset(), record.timestamp(), record.key(), v, h));
        verify(mockBuilder).appendWithOffset(record.offset(), record.timestamp(), record.key(), expectedValue, record.headers());
        assertThat(buffer.remaining()).isZero();
    }

    private record Header(
            @JsonProperty(required = true)
            ByteBuffer keyBase64,
            ByteBuffer valueBase64
    ) {
    }

    private record ParcelContents(
            ByteBuffer valueBase64,
            @JsonProperty(required = true)
            List<ParcelTest.Header> headers
    ) {
        org.apache.kafka.common.header.Header[] kafkaHeaders() {
            return this.headers.stream().map(header -> new RecordHeader(header.keyBase64(), header.valueBase64())).toArray(org.apache.kafka.common.header.Header[]::new);
        }
    }

    private record Exemplar(ByteBuffer serializedBase64) {
    }

    private record SerializationOptions(
            @JsonProperty(required = true)
            Set<RecordField> recordFields
    ) {
    }

    private record ParcelSerializationExemplar(
            @JsonProperty(required = true)
            ParcelContents originalRecordContents,
            @JsonProperty(required = true)
            ParcelContents deserializedParcelContents,
            @JsonProperty(required = true)
            SerializationOptions serializationOptions,
            Map<ParcelVersion, Exemplar> exemplars
    ) {

    }

    private record NamedExemplars(
            ParcelSerializationExemplar exemplars,
            String name
    ) {
        Stream<NamedExemplar> flatten() {
            ParcelContents originalRecordContents = exemplars.originalRecordContents;
            ParcelContents deserializedParcelContents = exemplars.deserializedParcelContents;
            return Arrays.stream(ParcelVersion.values()).map(version -> {
                Exemplar exemplar = exemplars.exemplars.get(version);
                return new NamedExemplar(originalRecordContents, deserializedParcelContents, version, exemplar, name, exemplars.serializationOptions);
            });
        }
    }

    private record NamedExemplar(
            ParcelContents originalRecordContents,
            ParcelContents deserializedParcelContents,
            ParcelVersion version,
            Exemplar exemplar,
            String name,
            SerializationOptions options
    ) {

        public ByteBuffer serialized() {
            return exemplar.serializedBase64();
        }

        public String serializedBase64() {
            return toBase64(serialized());
        }
    }

    private static final Pattern TEST_RESOURCE_FILTER = Pattern.compile("serialization/parcel/.*\\.yaml");

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    private static Stream<Arguments> exemplarStream() throws IOException {
        return ClassPath.from(ParcelTest.class.getClassLoader())
                        .getResources()
                        .stream()
                        .filter(ri -> TEST_RESOURCE_FILTER.matcher(ri.getResourceName()).matches())
                        .map(resourceInfo -> {
                            try {
                                ParcelSerializationExemplar parcelSerializationExemplar = MAPPER.reader()
                                                                                                .readValue(
                                                                                                        resourceInfo.asByteSource().openStream(),
                                                                                                        ParcelSerializationExemplar.class
                                                                                                );
                                return new NamedExemplars(parcelSerializationExemplar, resourceInfo.getResourceName());
                            }
                            catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        })
                        .flatMap(NamedExemplars::flatten)
                        .map(exemplar -> Arguments.of(exemplar.name + " - " + exemplar.version, exemplar));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("exemplarStream")
    void shouldDeserializeExpectedContentsFromBytes(String name, NamedExemplar exemplar) {
        failOnVersionWithoutExemplar(exemplar);
        Record mock = Mockito.mock(Record.class);
        when(mock.headers()).thenReturn(new org.apache.kafka.common.header.Header[0]);
        ParcelContents expected = exemplar.deserializedParcelContents;
        try {
            ParcelVersionResolver.ALL.fromName(exemplar.version).readParcel(exemplar.serialized(), mock, (byteBuffer, headers) -> {
                assertThat(expected.kafkaHeaders()).describedAs("headers").isEqualTo(headers);
                assertThat(toBase64(byteBuffer)).describedAs("parcel originalRecordContents buffer").isEqualTo(toBase64(expected.valueBase64));
            });
        }
        catch (Exception e) {
            fail("failed to deserialize parcel from bytes: " + exemplar.serializedBase64(), e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("exemplarStream")
    void shouldSerializeToExpectedBytes(String name, NamedExemplar exemplar) {
        failOnVersionWithoutExemplar(exemplar);
        String base64Encoded = serialize(exemplar.originalRecordContents, exemplar.version, exemplar.options);
        assertThat(base64Encoded).describedAs("serialized parcel").isEqualTo(exemplar.serializedBase64());
    }

    private static void failOnVersionWithoutExemplar(NamedExemplar exemplar) {
        if (exemplar.exemplar == null) {
            try {
                String base64Encoded = serialize(exemplar.originalRecordContents, exemplar.version, exemplar.options);
                fail("exemplar had no serialized binary for parcel version " + exemplar.version + ", serialized b64 output for this version is: " + base64Encoded);
            }
            catch (Exception e) {
                fail("exemplar had no serialized binary for parcel version " + exemplar.version + " and serialization with version failed", e);
            }
        }
    }

    private static String serialize(ParcelContents parcelContents, ParcelVersion version, SerializationOptions serializationOptions) {
        Record record = RecordTestUtils.record(parcelContents.valueBase64(), parcelContents.kafkaHeaders());
        ByteBuffer parcelBuffer = ByteBuffer.allocate(1024);
        ParcelVersionResolver.ALL.fromName(version).writeParcel(serializationOptions.recordFields, record, parcelBuffer);
        parcelBuffer.flip();
        return toBase64(parcelBuffer);
    }

    private static String toBase64(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] dst = new byte[buffer.limit()];
        buffer.get(dst);
        return Base64.getEncoder().encodeToString(dst);
    }

}
