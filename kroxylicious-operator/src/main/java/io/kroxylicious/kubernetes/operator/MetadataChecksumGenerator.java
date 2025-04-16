/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import io.fabric8.kubernetes.api.model.HasMetadata;

public class MetadataChecksumGenerator {

    private MetadataChecksumGenerator() {
    }

    public static String checksumFor(HasMetadata... metadataSources) {
        var checksum = new CRC32();
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[Long.BYTES]);
        for (HasMetadata metadataSource : metadataSources) {
            byteBuffer.clear();
            var objectMeta = metadataSource.getMetadata();
            checksum.update(objectMeta.getUid().getBytes());
            checksum.update(byteBuffer.putLong(objectMeta.getGeneration()).rewind());
        }
        return String.valueOf(checksum.getValue());
    }
}
