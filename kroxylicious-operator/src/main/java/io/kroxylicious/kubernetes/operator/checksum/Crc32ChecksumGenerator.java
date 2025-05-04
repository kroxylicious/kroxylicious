/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checksum;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.zip.CRC32;

import javax.annotation.concurrent.NotThreadSafe;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

@NotThreadSafe
public class Crc32ChecksumGenerator implements MetadataChecksumGenerator {

    private static final Base64.Encoder BASE_64_ENCODER = Base64.getEncoder().withoutPadding();
    private final CRC32 checksum;
    private final ByteBuffer byteBuffer;

    public Crc32ChecksumGenerator() {
        checksum = new CRC32();
        byteBuffer = ByteBuffer.wrap(new byte[Long.BYTES]);
    }

    @Override
    public void appendMetadata(ObjectMeta objectMeta) {
        appendString(objectMeta.getUid());
        appendVersionSpecifier(objectMeta);
        Map<String, String> annotations = objectMeta.getAnnotations();
        if (annotations != null && annotations.containsKey(REFERENT_CHECKSUM_ANNOTATION)) {
            appendString(annotations.get(REFERENT_CHECKSUM_ANNOTATION));
        }
    }

    @Override
    public void appendMetadata(HasMetadata entity) {
        appendMetadata(entity.getMetadata());
    }

    @Override
    public void appendString(String value) {
        checksum.update(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void appendLong(Long value) {
        byteBuffer.putLong(0, value);
        checksum.update(byteBuffer);
    }

    @Override
    public String encode() {
        long value = checksum.getValue();
        if (value == 0) {
            return NO_CHECKSUM_SPECIFIED;
        }
        byteBuffer.putLong(0, value);
        return BASE_64_ENCODER.encodeToString(byteBuffer.array());
    }
}
