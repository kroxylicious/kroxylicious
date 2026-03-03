/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checksum;

import java.nio.ByteBuffer;
import java.util.Base64;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public class FixedChecksumGenerator implements MetadataChecksumGenerator {

    private final String base64Value;

    public FixedChecksumGenerator(long checksum) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        byteBuffer.putLong(0, checksum);
        base64Value = Base64.getEncoder().withoutPadding().encodeToString(byteBuffer.array());
    }

    public void appendMetadata(ObjectMeta objectMeta) {
        // no-op as we already know the final result
    }

    @Override
    public void appendMetadata(HasMetadata entity) {
        // no-op as we already know the final result
    }

    @Override
    public void appendString(String value) {
        // no-op as we already know the final result
    }

    @Override
    public void appendLong(Long value) {
        // no-op as we already know the final result
    }

    @Override
    public String encode() {
        return base64Value;
    }
}
